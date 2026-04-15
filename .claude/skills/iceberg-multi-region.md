---
description: "Setup multi-region resilience for Iceberg tables: S3 replication, metadata repointing, cross-region Glue Catalog registration. Use when a producer needs disaster recovery or multi-region read access for their Iceberg tables. Cross-region S3 access and Multi-Region Access Points are NOT allowed."
user_invocable: true
---

# Iceberg Multi-Region Resilience

## Goal
Generate infrastructure and utility code to enable multi-region resilience for Iceberg tables. Since cross-region S3 access and Multi-Region Access Points are NOT allowed, this requires data replication, metadata repointing, and independent catalog registration in each region.

## CRITICAL CONSTRAINTS
- **Cross-region S3 access is NOT allowed** - workloads in region B cannot read S3 in region A
- **Multi-Region Access Points are NOT allowed**
- Each region must have a **complete local copy** of data and metadata
- Each region has its own **independent Glue Data Catalog**
- Iceberg metadata files contain **absolute S3 paths** that must be rewritten for the target region

## Architecture Overview

```
Source Region (e.g., us-east-1)          Target Region (e.g., us-west-2)
┌─────────────────────────┐              ┌─────────────────────────┐
│ Glue Catalog            │              │ Glue Catalog            │
│  └─ db.table            │              │  └─ db.table            │
│     (metadata loc →     │              │     (metadata loc →     │
│      source S3 bucket)  │              │      target S3 bucket)  │
└────────┬────────────────┘              └────────┬────────────────┘
         │                                        │
┌────────▼────────────────┐  S3 CRR     ┌────────▼────────────────┐
│ s3://prod-bucket-east/  │ ──────────► │ s3://prod-bucket-west/  │
│  warehouse/             │  (replicate │  warehouse/             │
│   ├─ metadata/          │   all files)│   ├─ metadata/          │
│   ├─ data/              │             │   │  (REPOINTED)        │
│   └─ ...                │             │   ├─ data/              │
└─────────────────────────┘             │   └─ ...                │
                                        └─────────────────────────┘
                                                  ▲
                                         Repointing Utility
                                         rewrites all S3 paths
                                         in metadata files from
                                         source → target bucket
```

## Step 1: Gather Requirements

Ask the producer (if not already provided):

1. **Source region and S3 bucket**: e.g., us-east-1, s3://producer-data-east/
2. **Target region(s) and S3 bucket(s)**: e.g., us-west-2, s3://producer-data-west/
3. **Tables to replicate**: database.table names
4. **Tech stack for repointing utility**: Python (Lambda/ECS) or Java (Lambda/ECS)?
5. **Sync frequency**: How often should metadata be synced? (after every commit, hourly, daily?)
6. **Infrastructure provisioning**: CloudFormation, CDK, Terraform, or manual?
7. **Current S3 versioning status**: Required for CRR
8. **Target region usage**: Read-only failover? Active-active? Read replicas?

## Step 2: Infrastructure Setup

### 2.1 S3 Cross-Region Replication (CRR)

**CloudFormation template:**
```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: S3 CRR for Iceberg multi-region resilience

Parameters:
  SourceBucketName:
    Type: String
  TargetBucketName:
    Type: String
  TargetRegion:
    Type: String
  WarehousePrefix:
    Type: String
    Default: 'warehouse/'

Resources:
  ReplicationRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: s3.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: S3ReplicationPolicy
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - s3:GetReplicationConfiguration
                  - s3:ListBucket
                Resource: !Sub 'arn:aws:s3:::${SourceBucketName}'
              - Effect: Allow
                Action:
                  - s3:GetObjectVersionForReplication
                  - s3:GetObjectVersionAcl
                  - s3:GetObjectVersionTagging
                Resource: !Sub 'arn:aws:s3:::${SourceBucketName}/${WarehousePrefix}*'
              - Effect: Allow
                Action:
                  - s3:ReplicateObject
                  - s3:ReplicateDelete
                  - s3:ReplicateTags
                Resource: !Sub 'arn:aws:s3:::${TargetBucketName}/${WarehousePrefix}*'

  SourceBucketReplicationConfig:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Ref SourceBucketName
      VersioningConfiguration:
        Status: Enabled
      ReplicationConfiguration:
        Role: !GetAtt ReplicationRole.Arn
        Rules:
          - Id: IcebergReplication
            Status: Enabled
            Prefix: !Ref WarehousePrefix
            Destination:
              Bucket: !Sub 'arn:aws:s3:::${TargetBucketName}'
              StorageClass: STANDARD
            DeleteMarkerReplication:
              Status: Disabled  # CRITICAL: Keep Disabled for Iceberg - source-side maintenance (expire snapshots, orphan cleanup) must NOT cascade deletes to the DR copy

  TargetBucket:
    Type: AWS::S3::Bucket
    Condition: CreateTargetBucket
    Properties:
      BucketName: !Ref TargetBucketName
      VersioningConfiguration:
        Status: Enabled
```

### 2.2 Target Region Glue Catalog Setup

```python
import boto3

def register_iceberg_table_in_target_catalog(
    target_region: str,
    database: str,
    table_name: str,
    target_bucket: str,
    warehouse_path: str,
    repointed_metadata_location: str
):
    """Register an Iceberg table in the target region's Glue Catalog."""
    glue = boto3.client('glue', region_name=target_region)

    # Ensure database exists
    try:
        glue.get_database(Name=database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={'Name': database, 'Description': f'Iceberg replica from source region'}
        )

    # Register Iceberg table pointing to repointed metadata
    table_input = {
        'Name': table_name,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'table_type': 'ICEBERG',
            'metadata_location': repointed_metadata_location,
            'format-version': '2'
        },
        'StorageDescriptor': {
            'Location': f's3://{target_bucket}/{warehouse_path}/{database}/{table_name}',
            'Columns': [],  # Iceberg manages schema via metadata
            'InputFormat': 'org.apache.hadoop.mapred.FileInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            }
        }
    }

    try:
        glue.get_table(DatabaseName=database, Name=table_name)
        glue.update_table(DatabaseName=database, TableInput=table_input)
        print(f"Updated table {database}.{table_name} in {target_region}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        print(f"Created table {database}.{table_name} in {target_region}")
```

## Step 3: Metadata Repointing Utility

This is the core utility. Iceberg metadata files contain absolute S3 paths. After S3 CRR replicates files to the target bucket, all paths still reference the source bucket. This utility rewrites them.

### Understanding Iceberg Metadata Structure
```
metadata/
  ├── v1.metadata.json         <- JSON: contains snapshot list, schema, manifest-list locations
  ├── v2.metadata.json         <- JSON: each commit creates a new version
  ├── snap-12345-abcdef.avro   <- AVRO: manifest-list, contains manifest file locations
  ├── abcdef-m0.avro           <- AVRO: manifest, contains data file locations
  └── ...
data/
  ├── partition=value/
  │   ├── 00001-data.parquet   <- Parquet data files
  │   └── ...
  └── ...
```

**What needs repointing:**
1. `metadata.json` (JSON) - references to manifest-list paths
2. `snap-*.avro` manifest-list files (Avro) - references to manifest paths
3. `*-m*.avro` manifest files (Avro) - references to data file paths

### Python Repointing Utility (Lambda / ECS)

```python
"""
Iceberg Metadata Repointing Utility

Rewrites all S3 path references in Iceberg metadata files from
source bucket to target bucket, enabling multi-region resilience.

Usage:
    repoint_metadata(
        source_bucket="prod-data-east",
        target_bucket="prod-data-west",
        target_region="us-west-2",
        database="mydb",
        table_name="mytable",
        warehouse_prefix="warehouse"
    )
"""

import json
import io
import boto3
import fastavro
from typing import Optional
from copy import deepcopy


class IcebergMetadataRepointer:
    """Rewrites Iceberg metadata to replace source S3 paths with target S3 paths."""

    def __init__(self, source_bucket: str, target_bucket: str, target_region: str):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.source_prefix = f"s3://{source_bucket}/"
        self.target_prefix = f"s3://{target_bucket}/"
        self.s3 = boto3.client('s3', region_name=target_region)

    def _replace_paths(self, text: str) -> str:
        """Replace all source bucket references with target bucket."""
        return text.replace(self.source_prefix, self.target_prefix)

    def repoint_metadata_json(self, metadata_key: str) -> str:
        """
        Repoint a metadata.json file.
        Returns the S3 key of the repointed metadata file.
        """
        # Read the replicated metadata.json from target bucket
        response = self.s3.get_object(Bucket=self.target_bucket, Key=metadata_key)
        metadata_content = response['Body'].read().decode('utf-8')

        # Replace all source paths with target paths
        repointed_content = self._replace_paths(metadata_content)

        # Write repointed metadata to target bucket
        # Use a new key to avoid overwriting the CRR-replicated original
        repointed_key = metadata_key.replace('.metadata.json', '.repointed.metadata.json')
        self.s3.put_object(
            Bucket=self.target_bucket,
            Key=repointed_key,
            Body=repointed_content.encode('utf-8'),
            ContentType='application/json'
        )
        return repointed_key

    def repoint_avro_file(self, avro_key: str) -> str:
        """
        Repoint an Avro file (manifest-list or manifest).
        Reads Avro records, replaces paths, writes new Avro file.
        Returns the S3 key of the repointed file.
        """
        # Read Avro file from target bucket
        response = self.s3.get_object(Bucket=self.target_bucket, Key=avro_key)
        avro_bytes = response['Body'].read()

        # Parse Avro
        reader = fastavro.reader(io.BytesIO(avro_bytes))
        schema = reader.writer_schema
        records = list(reader)

        # Replace paths in all string fields recursively
        repointed_records = []
        for record in records:
            repointed_records.append(self._repoint_record(record))

        # Write repointed Avro
        output = io.BytesIO()
        fastavro.writer(output, schema, repointed_records)
        output.seek(0)

        repointed_key = avro_key.replace('.avro', '.repointed.avro')
        self.s3.put_object(
            Bucket=self.target_bucket,
            Key=repointed_key,
            Body=output.read(),
            ContentType='application/avro'
        )
        return repointed_key

    def _repoint_record(self, record):
        """Recursively replace source paths in Avro record fields."""
        if isinstance(record, dict):
            return {k: self._repoint_record(v) for k, v in record.items()}
        elif isinstance(record, list):
            return [self._repoint_record(item) for item in record]
        elif isinstance(record, str):
            return self._replace_paths(record)
        return record

    def repoint_table(self, database: str, table_name: str, warehouse_prefix: str = "warehouse"):
        """
        Full table repointing workflow:
        1. Find latest metadata.json in target bucket
        2. Repoint metadata.json
        3. Parse metadata to find manifest-list files
        4. Repoint each manifest-list
        5. Parse manifest-lists to find manifest files
        6. Repoint each manifest
        7. Update repointed metadata.json with repointed manifest-list paths
        8. Return the final repointed metadata location
        """
        table_path = f"{warehouse_prefix}/{database}/{table_name}"
        metadata_path = f"{table_path}/metadata"

        # Step 1: Find latest metadata.json
        latest_metadata_key = self._find_latest_metadata(metadata_path)
        if not latest_metadata_key:
            raise ValueError(f"No metadata.json found at s3://{self.target_bucket}/{metadata_path}/")

        # Step 2: Read and parse metadata
        response = self.s3.get_object(Bucket=self.target_bucket, Key=latest_metadata_key)
        metadata = json.loads(response['Body'].read().decode('utf-8'))

        # Step 3: Collect all Avro files to repoint
        avro_files_to_repoint = set()
        repointed_avro_map = {}  # old_path -> new_path

        for snapshot in metadata.get('snapshots', []):
            manifest_list_location = snapshot.get('manifest-list', '')
            if manifest_list_location:
                avro_files_to_repoint.add(manifest_list_location)

        # Step 4: Repoint manifest-list files and collect manifest paths
        manifest_files = set()
        for avro_location in avro_files_to_repoint:
            # Convert S3 URI to key
            avro_key = self._s3_uri_to_key(avro_location)
            if not avro_key:
                continue

            # Read manifest-list to find manifests
            try:
                response = self.s3.get_object(Bucket=self.target_bucket, Key=avro_key)
                avro_bytes = response['Body'].read()
                reader = fastavro.reader(io.BytesIO(avro_bytes))
                for record in reader:
                    manifest_path = record.get('manifest_path', '')
                    if manifest_path:
                        manifest_files.add(manifest_path)
            except Exception as e:
                print(f"Warning: Could not read manifest list {avro_key}: {e}")

            # Repoint the manifest-list
            repointed_key = self.repoint_avro_file(avro_key)
            old_uri = f"s3://{self.source_bucket}/{avro_key}"
            new_uri = f"s3://{self.target_bucket}/{repointed_key}"
            repointed_avro_map[old_uri] = new_uri

        # Step 5: Repoint manifest files
        for manifest_location in manifest_files:
            manifest_key = self._s3_uri_to_key(manifest_location)
            if manifest_key:
                try:
                    repointed_key = self.repoint_avro_file(manifest_key)
                    old_uri = f"s3://{self.source_bucket}/{manifest_key}"
                    new_uri = f"s3://{self.target_bucket}/{repointed_key}"
                    repointed_avro_map[old_uri] = new_uri
                except Exception as e:
                    print(f"Warning: Could not repoint manifest {manifest_key}: {e}")

        # Step 6: Create final repointed metadata.json
        metadata_str = json.dumps(metadata)
        # Replace source bucket paths with target bucket
        metadata_str = self._replace_paths(metadata_str)
        # Replace original Avro paths with repointed Avro paths
        for old_path, new_path in repointed_avro_map.items():
            target_old_path = old_path.replace(self.source_prefix, self.target_prefix)
            metadata_str = metadata_str.replace(target_old_path, new_path)

        # Write final metadata
        final_metadata_key = latest_metadata_key.replace(
            '.metadata.json', '.multi-region.metadata.json'
        )
        self.s3.put_object(
            Bucket=self.target_bucket,
            Key=final_metadata_key,
            Body=metadata_str.encode('utf-8'),
            ContentType='application/json'
        )

        final_location = f"s3://{self.target_bucket}/{final_metadata_key}"
        print(f"Repointed metadata written to: {final_location}")
        return final_location

    def _find_latest_metadata(self, metadata_path: str) -> Optional[str]:
        """Find the latest metadata.json file in the metadata directory."""
        paginator = self.s3.get_paginator('list_objects_v2')
        metadata_files = []
        for page in paginator.paginate(Bucket=self.target_bucket, Prefix=metadata_path):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.metadata.json') and '.repointed.' not in key and '.multi-region.' not in key:
                    metadata_files.append(key)
        if not metadata_files:
            return None
        # Sort by version number (v1, v2, ...) or by last modified
        metadata_files.sort()
        return metadata_files[-1]

    def _s3_uri_to_key(self, s3_uri: str) -> Optional[str]:
        """Convert s3://bucket/key to just the key, handling both source and target bucket."""
        for prefix in [self.source_prefix, self.target_prefix]:
            if s3_uri.startswith(prefix):
                return s3_uri[len(prefix):]
        return None


def repoint_and_register(
    source_bucket: str,
    target_bucket: str,
    target_region: str,
    database: str,
    table_name: str,
    warehouse_prefix: str = "warehouse"
):
    """
    Complete multi-region setup: repoint metadata and register in target Glue Catalog.
    This is the main entry point for the repointing workflow.
    """
    repointer = IcebergMetadataRepointer(source_bucket, target_bucket, target_region)

    # Step 1: Repoint all metadata
    repointed_metadata_location = repointer.repoint_table(
        database, table_name, warehouse_prefix
    )

    # Step 2: Register in target region's Glue Catalog
    glue = boto3.client('glue', region_name=target_region)

    # Ensure database exists
    try:
        glue.get_database(Name=database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={
            'Name': database,
            'Description': f'Iceberg multi-region replica'
        })

    table_input = {
        'Name': table_name,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'table_type': 'ICEBERG',
            'metadata_location': repointed_metadata_location,
            'format-version': '2'
        },
        'StorageDescriptor': {
            'Location': f's3://{target_bucket}/{warehouse_prefix}/{database}/{table_name}',
            'Columns': [],
            'InputFormat': 'org.apache.hadoop.mapred.FileInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            }
        }
    }

    try:
        glue.get_table(DatabaseName=database, Name=table_name)
        glue.update_table(DatabaseName=database, TableInput=table_input)
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database, TableInput=table_input)

    print(f"Table {database}.{table_name} registered in {target_region} Glue Catalog")
    print(f"Metadata location: {repointed_metadata_location}")
    return repointed_metadata_location
```

### Java Repointing Utility (ECS / Lambda Java)

Maven dependencies for Java repointing utility:
```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>s3</artifactId>
    <version>2.25.0</version>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>glue</artifactId>
    <version>2.25.0</version>
</dependency>
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>1.11.3</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.17.0</version>
</dependency>
```

```java
package com.datamesh.iceberg.multiregion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IcebergMetadataRepointer {

    private final String sourceBucket;
    private final String targetBucket;
    private final String sourcePrefix;
    private final String targetPrefix;
    private final S3Client s3;
    private final GlueClient glue;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public IcebergMetadataRepointer(String sourceBucket, String targetBucket, String targetRegion) {
        this.sourceBucket = sourceBucket;
        this.targetBucket = targetBucket;
        this.sourcePrefix = "s3://" + sourceBucket + "/";
        this.targetPrefix = "s3://" + targetBucket + "/";
        this.s3 = S3Client.builder().region(Region.of(targetRegion)).build();
        this.glue = GlueClient.builder().region(Region.of(targetRegion)).build();
    }

    public String repointTable(String database, String tableName, String warehousePrefix) {
        String metadataPath = warehousePrefix + "/" + database + "/" + tableName + "/metadata";

        // 1. Find latest metadata.json
        String latestMetadataKey = findLatestMetadata(metadataPath);

        // 2. Read metadata
        String metadataContent = readS3String(targetBucket, latestMetadataKey);

        // 3. Replace all source paths
        String repointed = metadataContent.replace(sourcePrefix, targetPrefix);

        // 4. Write repointed metadata
        String repointedKey = latestMetadataKey.replace(
            ".metadata.json", ".multi-region.metadata.json"
        );
        writeS3String(targetBucket, repointedKey, repointed);

        // 5. Repoint Avro files (manifest-lists and manifests)
        repointAvroFiles(metadataPath);

        String metadataLocation = "s3://" + targetBucket + "/" + repointedKey;

        // 6. Register in Glue Catalog
        registerInGlueCatalog(database, tableName, targetBucket, warehousePrefix, metadataLocation);

        return metadataLocation;
    }

    private void repointAvroFiles(String metadataPath) {
        // List all .avro files in metadata directory
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
            .bucket(targetBucket).prefix(metadataPath + "/").build();
        ListObjectsV2Response listResp = s3.listObjectsV2(listReq);

        for (S3Object obj : listResp.contents()) {
            String key = obj.key();
            if (!key.endsWith(".avro") || key.contains(".repointed.")) continue;

            // Read Avro file from target bucket
            byte[] avroBytes = s3.getObject(
                GetObjectRequest.builder().bucket(targetBucket).key(key).build()
            ).readAllBytes();

            // Parse Avro records, replace source paths with target paths
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataReader = new DataFileReader<>(
                new SeekableByteArrayInput(avroBytes), reader
            );
            org.apache.avro.Schema avroSchema = dataReader.getSchema();
            List<GenericRecord> records = new ArrayList<>();
            while (dataReader.hasNext()) {
                records.add(dataReader.next());
            }
            dataReader.close();

            // Repoint string fields containing source bucket paths
            List<GenericRecord> repointed = records.stream()
                .map(this::repointAvroRecord)
                .collect(Collectors.toList());

            // Write repointed Avro
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
            DataFileWriter<GenericRecord> dataWriter = new DataFileWriter<>(writer);
            dataWriter.create(avroSchema, output);
            for (GenericRecord rec : repointed) {
                dataWriter.append(rec);
            }
            dataWriter.close();

            // Upload repointed file
            String repointedKey = key.replace(".avro", ".repointed.avro");
            s3.putObject(
                PutObjectRequest.builder().bucket(targetBucket).key(repointedKey).build(),
                RequestBody.fromBytes(output.toByteArray())
            );
        }
    }

    private GenericRecord repointAvroRecord(GenericRecord record) {
        // Recursively replace source bucket paths in all string fields
        GenericRecord copy = new GenericData.Record(record.getSchema());
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            if (value instanceof CharSequence) {
                copy.put(field.name(), value.toString().replace(sourcePrefix, targetPrefix));
            } else if (value instanceof GenericRecord) {
                copy.put(field.name(), repointAvroRecord((GenericRecord) value));
            } else {
                copy.put(field.name(), value);
            }
        }
        return copy;
    }

    private void registerInGlueCatalog(
        String database, String tableName, String bucket,
        String warehousePrefix, String metadataLocation
    ) {
        // Ensure database exists
        try {
            glue.getDatabase(GetDatabaseRequest.builder().name(database).build());
        } catch (EntityNotFoundException e) {
            glue.createDatabase(CreateDatabaseRequest.builder()
                .databaseInput(DatabaseInput.builder()
                    .name(database)
                    .description("Iceberg multi-region replica")
                    .build())
                .build());
        }

        // Create or update table
        TableInput tableInput = TableInput.builder()
            .name(tableName)
            .tableType("EXTERNAL_TABLE")
            .parameters(Map.of(
                "table_type", "ICEBERG",
                "metadata_location", metadataLocation,
                "format-version", "2"
            ))
            .storageDescriptor(StorageDescriptor.builder()
                .location("s3://" + bucket + "/" + warehousePrefix + "/" + database + "/" + tableName)
                .inputFormat("org.apache.hadoop.mapred.FileInputFormat")
                .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .serdeInfo(SerDeInfo.builder()
                    .serializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
                    .build())
                .build())
            .build();

        try {
            glue.getTable(GetTableRequest.builder()
                .databaseName(database).name(tableName).build());
            glue.updateTable(UpdateTableRequest.builder()
                .databaseName(database).tableInput(tableInput).build());
        } catch (EntityNotFoundException e) {
            glue.createTable(CreateTableRequest.builder()
                .databaseName(database).tableInput(tableInput).build());
        }
    }

    // ... helper methods: readS3String, writeS3String, findLatestMetadata
}
```

## Step 4: Sync Automation

### EventBridge + Lambda Sync (recommended)

```python
"""
Lambda function triggered by S3 events or on a schedule to sync
Iceberg metadata to the target region after new commits.
"""

import boto3
import json
from iceberg_repointer import IcebergMetadataRepointer  # the utility above

def handler(event, context):
    config = {
        'source_bucket': event.get('source_bucket') or os.environ['SOURCE_BUCKET'],
        'target_bucket': event.get('target_bucket') or os.environ['TARGET_BUCKET'],
        'target_region': event.get('target_region') or os.environ['TARGET_REGION'],
        'database': event.get('database') or os.environ['DATABASE'],
        'table_name': event.get('table_name') or os.environ['TABLE_NAME'],
    }

    repointer = IcebergMetadataRepointer(
        config['source_bucket'],
        config['target_bucket'],
        config['target_region']
    )

    metadata_location = repointer.repoint_table(
        config['database'],
        config['table_name']
    )

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Metadata repointed successfully',
            'metadata_location': metadata_location
        })
    }
```

### S3 Event Notification Trigger

```yaml
# CloudFormation: trigger repointing when new metadata.json arrives via CRR
MetadataEventRule:
  Type: AWS::Events::Rule
  Properties:
    EventPattern:
      source: ["aws.s3"]
      detail-type: ["Object Created"]
      detail:
        bucket:
          name: [!Ref TargetBucketName]
        object:
          key:
            - prefix: !Sub "${WarehousePrefix}/"
            - suffix: ".metadata.json"
    Targets:
      - Arn: !GetAtt RepointingLambda.Arn
        Id: RepointMetadata
```

### Scheduled Sync (EventBridge Scheduler)

```yaml
ScheduledSyncRule:
  Type: AWS::Scheduler::Schedule
  Properties:
    ScheduleExpression: "rate(1 hour)"
    FlexibleTimeWindow:
      Mode: "OFF"
    Target:
      Arn: !GetAtt RepointingLambda.Arn
      Input: !Sub |
        {
          "source_bucket": "${SourceBucketName}",
          "target_bucket": "${TargetBucketName}",
          "target_region": "${TargetRegion}",
          "database": "${Database}",
          "table_name": "${TableName}"
        }
```

## Step 5: Monitoring & Validation

Generate monitoring for:

1. **Replication lag**: CloudWatch metric for S3 CRR pending operations
2. **Metadata freshness**: Compare latest snapshot timestamp between regions
3. **Data consistency**: Row count comparison between regions
4. **Repointing success**: CloudWatch alarms on Lambda errors

```python
def validate_multi_region_sync(source_region, target_region, database, table_name, warehouse):
    """Validate that source and target regions are in sync."""
    from pyiceberg.catalog.glue import GlueCatalog

    source_catalog = GlueCatalog("source", **{"warehouse": warehouse, "region_name": source_region})
    target_catalog = GlueCatalog("target", **{"warehouse": warehouse, "region_name": target_region})

    source_table = source_catalog.load_table(f"{database}.{table_name}")
    target_table = target_catalog.load_table(f"{database}.{table_name}")

    source_snapshot = source_table.current_snapshot()
    target_snapshot = target_table.current_snapshot()

    lag_ms = source_snapshot.timestamp_ms - target_snapshot.timestamp_ms
    print(f"Source snapshot: {source_snapshot.snapshot_id} at {source_snapshot.timestamp_ms}")
    print(f"Target snapshot: {target_snapshot.snapshot_id} at {target_snapshot.timestamp_ms}")
    print(f"Replication lag: {lag_ms / 1000:.0f} seconds")

    return {
        'source_snapshot': source_snapshot.snapshot_id,
        'target_snapshot': target_snapshot.snapshot_id,
        'lag_seconds': lag_ms / 1000,
        'in_sync': source_snapshot.snapshot_id == target_snapshot.snapshot_id
    }
```

## Step 6: Failover Procedure

Generate a runbook for failing over to the target region:

1. **Verify target is up-to-date**: Run validation check
2. **Update DNS/config**: Point applications to target region
3. **If writes needed in target**: Ensure no writes happening in source
4. **After failover**: Target becomes the new source for subsequent replication
5. **Failback**: Reverse the replication direction
