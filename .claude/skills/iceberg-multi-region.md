---
description: "Setup multi-region resilience for Iceberg tables: S3 replication, metadata repointing, cross-region Glue Catalog registration. Use when a producer needs disaster recovery or multi-region read access for their Iceberg tables. Cross-region S3 access and Multi-Region Access Points are NOT allowed."
---

# Iceberg Multi-Region Resilience

## Goal
Generate infrastructure and utility code to enable multi-region resilience for Iceberg tables. Since cross-region S3 access and Multi-Region Access Points are NOT allowed, this requires data replication, metadata repointing, and independent catalog registration in each region.

> **Scope of this skill vs. the `iceberg-multi-region-planner` agent:** this skill produces *the concrete code and Terraform* a producer can deploy for a single, well-defined multi-region setup. The planner agent is a step earlier — it does *requirements elicitation, pattern selection (active-passive vs active-active reads), RPO/RTO trade-off analysis, cost estimation, and the failover runbook*. Invoke the planner when the producer hasn't yet decided on the architecture; invoke this skill once they have. During orchestrated onboarding the orchestrator spawns the planner first, then this skill uses the planner's decisions as its inputs.

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
6. **Infrastructure provisioning**: Terraform (default), CDK, or manual? (CloudFormation is not used on this platform.)
7. **Current S3 versioning status**: Required for CRR
8. **Target region usage**: Read-only failover? Active-active? Read replicas?

## Step 2: Infrastructure Setup

### 2.1 S3 Cross-Region Replication (CRR)

**Terraform (CloudFormation is not used on this platform):**

```hcl
variable "source_bucket_name" { type = string }
variable "target_bucket_name" { type = string }
variable "source_region"      { type = string }
variable "target_region"      { type = string }
variable "warehouse_prefix"   { type = string  default = "warehouse/" }
# Optional: SSE-KMS key ARNs. Leave null if buckets use SSE-S3.
variable "source_kms_key_arn" { type = string  default = null }
variable "target_kms_key_arn" { type = string  default = null }

# Providers: one per region. The source bucket + its replication config and the
# replication IAM role live in the source region; the target bucket in the target region.
provider "aws" {
  alias  = "source"
  region = var.source_region
}
provider "aws" {
  alias  = "target"
  region = var.target_region
}

# --- Target bucket (DR copy) ---
resource "aws_s3_bucket" "target" {
  provider = aws.target
  bucket   = var.target_bucket_name
}

resource "aws_s3_bucket_versioning" "target" {
  provider = aws.target
  bucket   = aws_s3_bucket.target.id
  versioning_configuration { status = "Enabled" }
}

# --- Versioning on source (required for CRR) ---
resource "aws_s3_bucket_versioning" "source" {
  provider = aws.source
  bucket   = var.source_bucket_name
  versioning_configuration { status = "Enabled" }
}

# --- Replication role (S3 assumes this to copy objects cross-region) ---
resource "aws_iam_role" "replication" {
  provider = aws.source
  name     = "${var.source_bucket_name}-crr-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "replication" {
  provider = aws.source
  role     = aws_iam_role.replication.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Effect = "Allow"
          Action = [
            "s3:GetReplicationConfiguration",
            "s3:ListBucket",
            "s3:GetObjectVersionForReplication",
            "s3:GetObjectVersionAcl",
            "s3:GetObjectVersionTagging"
          ]
          Resource = [
            "arn:aws:s3:::${var.source_bucket_name}",
            "arn:aws:s3:::${var.source_bucket_name}/${var.warehouse_prefix}*"
          ]
        },
        {
          Effect = "Allow"
          Action = [
            "s3:ReplicateObject",
            "s3:ReplicateDelete",
            "s3:ReplicateTags",
            "s3:ObjectOwnerOverrideToBucketOwner"
          ]
          Resource = "arn:aws:s3:::${var.target_bucket_name}/${var.warehouse_prefix}*"
        }
      ],
      # If source objects are SSE-KMS, S3 replication needs Decrypt on the source key
      # and Encrypt/GenerateDataKey on the target key.
      var.source_kms_key_arn == null ? [] : [{
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = var.source_kms_key_arn
      }],
      var.target_kms_key_arn == null ? [] : [{
        Effect   = "Allow"
        Action   = ["kms:Encrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = var.target_kms_key_arn
      }]
    )
  })
}

# --- Replication config on source bucket ---
resource "aws_s3_bucket_replication_configuration" "iceberg" {
  provider = aws.source
  # Replication requires versioning enabled on the source first.
  depends_on = [aws_s3_bucket_versioning.source, aws_s3_bucket_versioning.target]
  role       = aws_iam_role.replication.arn
  bucket     = var.source_bucket_name

  rule {
    id     = "IcebergReplication"
    status = "Enabled"

    filter { prefix = var.warehouse_prefix }

    # CRITICAL: keep DeleteMarkerReplication DISABLED for Iceberg.
    # Source-side maintenance (expire_snapshots, remove_orphan_files, rewrite_data_files)
    # issues deletes on obsolete files. Cascading those deletes to the DR copy would
    # prune files the DR still needs for older snapshots, breaking replica independence.
    delete_marker_replication { status = "Disabled" }

    destination {
      bucket        = "arn:aws:s3:::${var.target_bucket_name}"
      storage_class = "STANDARD"

      # Re-encrypt replicated objects with the target-region KMS key when applicable.
      dynamic "encryption_configuration" {
        for_each = var.target_kms_key_arn == null ? [] : [var.target_kms_key_arn]
        content { replica_kms_key_id = encryption_configuration.value }
      }
    }

    # Re-encrypt the CRR source selection only if source is SSE-KMS.
    dynamic "source_selection_criteria" {
      for_each = var.source_kms_key_arn == null ? [] : [1]
      content {
        sse_kms_encrypted_objects { status = "Enabled" }
      }
    }
  }
}
```

### 2.2 Target Region Glue Catalog Setup

Use Iceberg's catalog `register_table` API (via PyIceberg) rather than raw `boto3.client('glue').create_table`. `register_table` writes the exact parameter set Iceberg engines require (`table_type=ICEBERG`, `metadata_location`, `previous_metadata_location`, schema columns, partition info) directly from the metadata.json. A hand-crafted `StorageDescriptor` with an empty `Columns: []` list is valid for Iceberg-aware readers but is not a contract — producers have hit edge cases where Athena DDL or external catalogs reject such entries. Prefer the Iceberg API so the target-region registration is byte-for-byte what the source-region registration would be.

```python
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError

def register_iceberg_table_in_target_catalog(
    target_region: str,
    database: str,
    table_name: str,
    target_bucket: str,
    warehouse_path: str,
    repointed_metadata_location: str,
):
    """Register an Iceberg table in the target region's Glue Catalog using Iceberg's register_table API."""
    catalog = GlueCatalog(
        "target",
        **{
            "warehouse": f"s3://{target_bucket}/{warehouse_path}/",
            # Canonical PyIceberg keys (both required: Glue for catalog ops, S3 for FileIO):
            "glue.region": target_region,
            "s3.region": target_region,
        },
    )

    # Ensure database exists (idempotent)
    try:
        catalog.create_namespace(database)
    except Exception:
        # Namespace already exists; fine.
        pass

    identifier = (database, table_name)
    try:
        # Fresh registration: catalog reads metadata.json and builds the correct Glue TableInput.
        catalog.register_table(identifier, repointed_metadata_location)
        print(f"Registered {database}.{table_name} in {target_region}")
    except TableAlreadyExistsError:
        # Table is already registered -- advance its metadata pointer to the new repointed location.
        # PyIceberg does not yet expose a pure "move pointer" API on GlueCatalog, so drop + re-register.
        # drop_table(purge=False) keeps the underlying S3 objects intact; only the catalog entry is removed.
        catalog.drop_table(identifier, purge=False)
        catalog.register_table(identifier, repointed_metadata_location)
        print(f"Re-registered {database}.{table_name} in {target_region} at new metadata location")
```

**Java equivalent** (uses the Iceberg `GlueCatalog` Java API, not the AWS SDK `GlueClient` directly):

```java
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

import java.util.Map;

public static void registerInTargetCatalog(
    String targetRegion, String database, String tableName,
    String targetBucket, String warehousePath, String repointedMetadataLocation
) {
    GlueCatalog catalog = new GlueCatalog();
    catalog.initialize("target", Map.of(
        "warehouse",   "s3://" + targetBucket + "/" + warehousePath + "/",
        "glue.region", targetRegion,
        "s3.region",   targetRegion,
        "io-impl",     "org.apache.iceberg.aws.s3.S3FileIO"
    ));

    Namespace ns = Namespace.of(database);
    try {
        catalog.createNamespace(ns);
    } catch (AlreadyExistsException ignored) { }

    TableIdentifier id = TableIdentifier.of(database, tableName);
    try {
        catalog.registerTable(id, repointedMetadataLocation);
    } catch (AlreadyExistsException e) {
        // Advance metadata pointer: drop (purge=false preserves S3 files) + re-register.
        catalog.dropTable(id, false);
        catalog.registerTable(id, repointedMetadataLocation);
    }
}
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
1. `metadata.json` (JSON) - references to manifest-list paths, previous-metadata-log entries, and `statistics-files` (Puffin) locations if present
2. `snap-*.avro` manifest-list files (Avro) - references to manifest paths
3. `*-m*.avro` manifest files (Avro) - references to data file paths and delete file paths

**Important**: Iceberg format-version 2 can include `statistics-files` entries in `metadata.json` that point to Puffin files (e.g., `s3://bucket/.../abc.stats`). The whole-string replace on the JSON content catches these; do not strip them. If you parse and rebuild selectively, preserve the `statistics-files` array.

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
        Preserves the original codec (deflate/snappy/null) to avoid file-size bloat.
        Returns the S3 key of the repointed file.
        """
        # Read Avro file from target bucket
        response = self.s3.get_object(Bucket=self.target_bucket, Key=avro_key)
        avro_bytes = response['Body'].read()

        # Parse Avro
        reader = fastavro.reader(io.BytesIO(avro_bytes))
        schema = reader.writer_schema
        # Preserve original codec; fastavro exposes it via reader.codec
        codec = getattr(reader, 'codec', 'deflate') or 'deflate'
        records = list(reader)

        # Replace paths in all string fields recursively
        repointed_records = []
        for record in records:
            repointed_records.append(self._repoint_record(record))

        # Write repointed Avro with the original codec
        output = io.BytesIO()
        fastavro.writer(output, schema, repointed_records, codec=codec)
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
        """
        Find the latest metadata.json file in the metadata directory.
        Sorts by S3 LastModified (authoritative) rather than filename lexical order,
        because legacy v1/v2/v10 naming sorts v10 < v2 lexically and breaks sort-by-name.
        """
        paginator = self.s3.get_paginator('list_objects_v2')
        metadata_files = []
        for page in paginator.paginate(Bucket=self.target_bucket, Prefix=metadata_path):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.metadata.json') and '.repointed.' not in key and '.multi-region.' not in key:
                    metadata_files.append((obj['LastModified'], key))
        if not metadata_files:
            return None
        metadata_files.sort(key=lambda t: t[0])
        return metadata_files[-1][1]

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

    # Step 2: Register (or re-register) in target region's Glue Catalog
    # using Iceberg's register_table API -- see Section 2.2 for the full helper.
    register_iceberg_table_in_target_catalog(
        target_region=target_region,
        database=database,
        table_name=table_name,
        target_bucket=target_bucket,
        warehouse_path=warehouse_prefix,
        repointed_metadata_location=repointed_metadata_location,
    )

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
        // List ALL .avro files in the metadata directory, paginating through every page.
        // listObjectsV2 returns at most 1000 keys; large tables (many snapshots × manifests)
        // blow past that easily. Missing a manifest file means the DR table is broken for
        // any snapshot whose manifests weren't repointed -- silent data-loss on failover.
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
            .bucket(targetBucket).prefix(metadataPath + "/").build();

        for (ListObjectsV2Response page : s3.listObjectsV2Paginator(listReq)) {
            for (S3Object obj : page.contents()) {
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
                // Preserve the source file's compression codec. DataFileWriter defaults to null
                // (uncompressed), which would inflate manifest files several-fold and diverges
                // from what Iceberg writes natively (deflate). The DataFileReader exposes the
                // codec via the reserved avro.codec metadata field.
                String codecName = dataReader.getMetaString("avro.codec");
                if (codecName == null || codecName.isEmpty()) {
                    codecName = "deflate";
                }

                List<GenericRecord> records = new ArrayList<>();
                while (dataReader.hasNext()) {
                    records.add(dataReader.next());
                }
                dataReader.close();

                // Repoint string fields containing source bucket paths
                List<GenericRecord> repointed = records.stream()
                    .map(this::repointAvroRecord)
                    .collect(Collectors.toList());

                // Write repointed Avro with the original codec.
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
                DataFileWriter<GenericRecord> dataWriter = new DataFileWriter<>(writer);
                dataWriter.setCodec(org.apache.avro.file.CodecFactory.fromString(codecName));
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
        // Use Iceberg's GlueCatalog.registerTable -- it reads metadata.json and writes the
        // correct Glue TableInput (ICEBERG parameters, schema columns, partition spec).
        // This avoids hand-crafting StorageDescriptor.Columns = [] which is not a stable contract.
        org.apache.iceberg.aws.glue.GlueCatalog icebergCatalog = new org.apache.iceberg.aws.glue.GlueCatalog();
        icebergCatalog.initialize("target", Map.of(
            "warehouse",   "s3://" + bucket + "/" + warehousePrefix + "/",
            "glue.region", Region.of(glue.serviceClientConfiguration().region().id()).id(),
            "s3.region",   Region.of(glue.serviceClientConfiguration().region().id()).id(),
            "io-impl",     "org.apache.iceberg.aws.s3.S3FileIO"
        ));

        org.apache.iceberg.catalog.Namespace ns = org.apache.iceberg.catalog.Namespace.of(database);
        try {
            icebergCatalog.createNamespace(ns);
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) { }

        org.apache.iceberg.catalog.TableIdentifier id =
            org.apache.iceberg.catalog.TableIdentifier.of(database, tableName);
        try {
            icebergCatalog.registerTable(id, metadataLocation);
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
            // Advance pointer: drop (purge=false keeps S3 files) + re-register at new metadata location.
            icebergCatalog.dropTable(id, false);
            icebergCatalog.registerTable(id, metadataLocation);
        }
    }

    // ... helper methods: readS3String, writeS3String, findLatestMetadata
}
```

> Maven: add `iceberg-aws-bundle` (or `iceberg-aws` + `iceberg-core`) as shown in the pipeline skill, so `org.apache.iceberg.aws.glue.GlueCatalog` is on the classpath.

## Step 4: Sync Automation

### EventBridge + Lambda Sync (recommended)

```python
"""
Lambda function triggered by S3 events or on a schedule to sync
Iceberg metadata to the target region after new commits.
"""

import os
import json
from iceberg_repointer import IcebergMetadataRepointer  # the utility above


def _triggering_key(event):
    """Extract the S3 object key from an EventBridge S3 Object Created event, if present.
    Scheduled (EventBridge Scheduler) invocations have no such key and return None."""
    detail = event.get('detail') or {}
    obj = detail.get('object') or {}
    return obj.get('key')


def handler(event, context):
    # Re-entry guard: the S3 Event rule filters on suffix ".metadata.json", which also
    # matches our own output files ".repointed.metadata.json" and ".multi-region.metadata.json".
    # Without this guard, every repointing run re-triggers the rule and we spin in a loop.
    key = _triggering_key(event)
    if key and ('.repointed.' in key or '.multi-region.' in key):
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Skipped repointer-generated key: {key}'})
        }

    config = {
        'source_bucket':    event.get('source_bucket')    or os.environ['SOURCE_BUCKET'],
        'target_bucket':    event.get('target_bucket')    or os.environ['TARGET_BUCKET'],
        'target_region':    event.get('target_region')    or os.environ['TARGET_REGION'],
        'database':         event.get('database')         or os.environ['DATABASE'],
        'table_name':       event.get('table_name')       or os.environ['TABLE_NAME'],
        # warehouse_prefix must be forwarded -- repoint_table defaults to "warehouse",
        # but producers may use "iceberg/" or a bucket-subpath, so don't rely on the default.
        'warehouse_prefix': event.get('warehouse_prefix') or os.environ.get('WAREHOUSE_PREFIX', 'warehouse'),
    }

    repointer = IcebergMetadataRepointer(
        config['source_bucket'],
        config['target_bucket'],
        config['target_region']
    )

    metadata_location = repointer.repoint_table(
        config['database'],
        config['table_name'],
        warehouse_prefix=config['warehouse_prefix'],
    )

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Metadata repointed successfully',
            'metadata_location': metadata_location
        })
    }
```

**Make sure the Lambda Terraform env vars and the Scheduler input both include `warehouse_prefix` / `WAREHOUSE_PREFIX`** -- the scheduled-sync target `input` block (below) already adds it, and the Lambda's `environment.variables` in the `aws_lambda_function.repointing` resource should set `WAREHOUSE_PREFIX = var.warehouse_prefix` so the event-driven path resolves correctly when no explicit input is passed.

### S3 Event Notification Trigger (Terraform)

Fire the repointing Lambda as soon as a new `metadata.json` shows up in the target bucket (via CRR). Requires the target bucket to have EventBridge notifications enabled.

```hcl
# Enable EventBridge notifications on the target bucket (one-time, target region).
resource "aws_s3_bucket_notification" "target_eventbridge" {
  provider    = aws.target
  bucket      = aws_s3_bucket.target.id
  eventbridge = true
}

# Rule: match Object Created events for *.metadata.json under the warehouse prefix.
resource "aws_cloudwatch_event_rule" "metadata_created" {
  provider    = aws.target
  name        = "iceberg-metadata-created-${var.target_bucket_name}"
  description = "Trigger repointing when new Iceberg metadata.json arrives via CRR"
  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.target_bucket_name] }
      object = {
        key = [{
          prefix = var.warehouse_prefix
          suffix = ".metadata.json"
        }]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "repoint_lambda" {
  provider  = aws.target
  rule      = aws_cloudwatch_event_rule.metadata_created.name
  target_id = "RepointMetadata"
  arn       = aws_lambda_function.repointing.arn
}

resource "aws_lambda_permission" "allow_eventbridge_rule" {
  provider      = aws.target
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.repointing.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.metadata_created.arn
}
```

> The event rule fires once per replicated metadata.json. If CRR happens to replicate several metadata versions in close succession (e.g., a burst of commits), each invocation still calls `_find_latest_metadata` which picks the newest by S3 LastModified — so multiple rapid fires converge on the latest snapshot.

### Scheduled Sync (EventBridge Scheduler, Terraform)

Use this instead of (or alongside) the event-driven trigger when RPO tolerates minutes-to-hours and you want a steady, predictable cadence.

```hcl
resource "aws_iam_role" "scheduler" {
  provider = aws.target
  name     = "iceberg-repoint-scheduler-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_invoke_lambda" {
  provider = aws.target
  role     = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.repointing.arn
    }]
  })
}

resource "aws_scheduler_schedule" "hourly_repoint" {
  provider                     = aws.target
  name                         = "iceberg-repoint-${var.database}-${var.table_name}"
  schedule_expression          = "rate(1 hour)"
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }

  target {
    arn      = aws_lambda_function.repointing.arn
    role_arn = aws_iam_role.scheduler.arn
    input = jsonencode({
      source_bucket    = var.source_bucket_name
      target_bucket    = var.target_bucket_name
      target_region    = var.target_region
      database         = var.database
      table_name       = var.table_name
      warehouse_prefix = var.warehouse_prefix
    })
  }
}
```

> Both triggers reference `aws_lambda_function.repointing` — the minimal block below is sufficient. For very large tables (>100k manifest entries) where rewriting all manifest Avro files exceeds Lambda's 15-minute ceiling, substitute an ECS Fargate task using the same handler code; the `/iceberg-pipeline` Section 3G ECS skeleton is a drop-in starting point.

**Repointing Lambda (Terraform)**:

```hcl
resource "aws_lambda_function" "repointing" {
  function_name = "${var.producer_name}-iceberg-repointing"
  role          = aws_iam_role.repointing_lambda.arn
  handler       = "repoint_handler.handler"
  runtime       = "python3.11"
  filename      = var.repointing_code_zip_path  # must bundle pyiceberg, fastavro
  timeout       = 900       # 15 min Lambda max.
  memory_size   = 3008      # Avro rewrites are CPU/memory heavy.
  ephemeral_storage { size = 4096 }  # /tmp for Avro buffering.
  environment {
    variables = {
      SOURCE_BUCKET    = var.source_bucket
      TARGET_BUCKET    = var.target_bucket
      SOURCE_REGION    = var.source_region
      TARGET_REGION    = var.target_region
      DATABASE         = var.database
      TABLE_NAME       = var.table_name
      WAREHOUSE_PREFIX = var.warehouse_prefix  # forwarded to repointer; see handler guard
    }
  }
  # Single-writer: only one repointing run may advance the target catalog pointer
  # at a time. EventBridge bursts from CRR batches MUST NOT be processed in parallel
  # or the catalog's current metadata_location can regress to an older snapshot.
  reserved_concurrent_executions = 1
}
```

The IAM role (`aws_iam_role.repointing_lambda`) needs: `s3:GetObject` on source bucket, `s3:GetObject`/`s3:PutObject`/`s3:ListBucket`/`s3:DeleteObject` on target bucket, `glue:GetTable`/`glue:UpdateTable`/`glue:CreateTable` in the target region, and `kms:Decrypt`/`kms:GenerateDataKey`/`kms:Encrypt` on any CMK protecting either bucket. Follow the same pattern as `aws_iam_role_policy.maint` in `/iceberg-pipeline` Section 3G, adjusting the bucket ARNs to `source_bucket` (read-only) and `target_bucket` (read-write).

### Orphan cleanup in the target bucket

Each repointing run writes new artifacts into the target bucket that are NOT managed by Iceberg's own maintenance procedures (because they are not referenced by the source-region metadata tree):

1. `*.repointed.avro` — rewritten manifest / manifest-list files from every prior repointing run.
2. `*.multi-region.metadata.json` — the repointed metadata.json that the target Glue Catalog currently points to, PLUS every superseded version from prior runs.
3. `*.metadata.json` and `snap-*.avro` / `*-m*.avro` from the source — these are the CRR-replicated originals. Do NOT delete them; they are the immutable inputs each repointing run reads from.

Do NOT run `system.remove_orphan_files` against the target-region table. Iceberg's orphan cleanup considers "orphan" to mean "not referenced by any live snapshot in the current metadata tree," and the `.repointed.avro` files ARE referenced by the current `.multi-region.metadata.json` — so the procedure would leave them alone, while also potentially misidentifying CRR-replicated source metadata files as orphans and deleting them. Both outcomes are wrong for a DR replica.

Use a dedicated cleanup pass instead. Safe rules:

```python
def cleanup_superseded_repointed_artifacts(
    s3_client, target_bucket: str, warehouse_prefix: str,
    database: str, table_name: str, retention_days: int = 7,
):
    """
    Delete *.repointed.avro and *.multi-region.metadata.json files that:
      - are not the current metadata location in the target Glue Catalog, AND
      - were last modified more than `retention_days` ago.

    Retaining a window of old repointed versions lets consumers mid-read on an older
    snapshot still resolve paths; this mirrors Iceberg's standard expire_snapshots grace period.
    """
    from datetime import datetime, timedelta, timezone
    from pyiceberg.catalog.glue import GlueCatalog

    catalog = GlueCatalog("target", **{
        "warehouse":   f"s3://{target_bucket}/{warehouse_prefix}/",
        "glue.region": s3_client.meta.region_name,
        "s3.region":   s3_client.meta.region_name,
    })
    current_metadata_location = catalog.load_table((database, table_name)).metadata_location
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    prefix = f"{warehouse_prefix}/{database}/{table_name}/metadata/"
    paginator = s3_client.get_paginator('list_objects_v2')
    to_delete = []
    for page in paginator.paginate(Bucket=target_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            is_repointed_artifact = (
                key.endswith('.repointed.avro') or
                '.multi-region.metadata.json' in key
            )
            if not is_repointed_artifact:
                continue
            full_uri = f"s3://{target_bucket}/{key}"
            if full_uri == current_metadata_location:
                continue  # Never delete the currently-registered metadata.
            if obj['LastModified'] >= cutoff:
                continue  # Within retention window.
            to_delete.append({'Key': key})

    # Batch delete (max 1000 keys per request).
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i+1000]
        if batch:
            s3_client.delete_objects(Bucket=target_bucket, Delete={'Objects': batch})
    return len(to_delete)
```

Schedule this as a low-frequency job (e.g., daily EventBridge Scheduler -> Lambda). Keep `retention_days >= source_region_max_snapshot_age_days` so a failover never lands on a snapshot whose repointed manifests were already pruned.

## Step 5: Monitoring & Validation

Generate monitoring for:

1. **Replication lag**: CloudWatch metric for S3 CRR pending operations
2. **Metadata freshness**: Compare latest snapshot timestamp between regions
3. **Data consistency**: Row count comparison between regions
4. **Repointing success**: CloudWatch alarms on Lambda errors

```python
def validate_multi_region_sync(source_region, target_region, database, table_name,
                               source_warehouse, target_warehouse):
    """Validate that source and target regions are in sync.

    source_warehouse and target_warehouse differ because each region has its own bucket
    (cross-region S3 is not allowed). Use canonical PyIceberg config keys (glue.region / s3.region).
    """
    from pyiceberg.catalog.glue import GlueCatalog

    source_catalog = GlueCatalog("source", **{
        "warehouse": source_warehouse,
        "glue.region": source_region,
        "s3.region":   source_region,
    })
    target_catalog = GlueCatalog("target", **{
        "warehouse": target_warehouse,
        "glue.region": target_region,
        "s3.region":   target_region,
    })

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
