---
description: "Generate end-to-end Iceberg pipeline for a producer. Use when a new producer needs a complete pipeline from scratch, or when an existing producer needs to add Iceberg support to their existing AWS Glue/EMR/ECS/Lambda pipeline."
user_invocable: true
---

# Iceberg Pipeline Generator

## Goal
Generate a complete, production-ready Iceberg data pipeline tailored to the producer's tech stack and requirements. Handles both greenfield (new) and brownfield (existing) producers.

## Step 1: Producer Assessment

Ask the producer (if not already provided):

1. **Producer type**: New (no existing pipeline) or Existing (has pipeline to augment)?
2. **Tech stack**:
   - New producer: Which stack to use? (Glue PySpark / EMR PySpark / ECS Python / Lambda Python / ECS Java / Lambda Java)
   - Existing producer: What do they currently use?
3. **Data source**: Where does data come from?
   - S3 (batch files), JDBC/database, API, Kafka, Kinesis, SQS, other?
   - Push (data arrives) or pull (pipeline fetches)?
4. **Data characteristics**:
   - Estimated volume per batch/day
   - Schema (provide or infer from sample)
   - Update pattern: append-only, upsert, full refresh?
   - Merge/primary key columns (if upsert)
5. **Partitioning**: By date? By category? Recommendations needed?
6. **Target table**: database name, table name, S3 bucket, region
7. **Multi-region**: Is multi-region resilience needed? Which target region(s)?
8. **Scheduling**: How often does the pipeline run?
9. **Monitoring**: CloudWatch, SNS alerts, custom?

## Step 2: Pipeline Architecture

Based on the assessment, design the pipeline with these components:

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Data Source  │ ──► │  Ingestion   │ ──► │  Iceberg     │ ──► │ Maintenance  │
│             │     │  Job         │     │  Table       │     │  Job         │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                          │                     │                     │
                          │              ┌──────▼──────┐              │
                          │              │ Multi-Region│              │
                          │              │ Sync        │              │
                          │              └─────────────┘              │
                    ┌─────▼─────┐                            ┌───────▼──────┐
                    │ Scheduling│                            │  Monitoring  │
                    │ (EventBridge/Step Functions)           │  & Alerts    │
                    └───────────┘                            └──────────────┘
```

### Components to Generate

| Component | New Producer | Existing Producer |
|---|---|---|
| Table DDL | Full CREATE TABLE | Verify/update existing |
| Spark/Catalog config | Full configuration | Add Iceberg configs |
| Ingestion code | Full pipeline | Modify write operations |
| Maintenance job | New job | New job (likely missing) |
| Multi-region sync | Full setup | Full setup |
| Scheduling | New schedule | Update schedule |
| IAM roles | New roles | Update policies |
| Monitoring | Full setup | Add Iceberg metrics |

## Step 3: Generate Pipeline Code

### 3A: New Producer - AWS Glue PySpark

Generate a complete Glue job with the following files:

**1. Glue Job Script (`glue_iceberg_pipeline.py`)**:
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'database', 'table_name', 'warehouse',
    'source_path', 'operation'  # operation: append, upsert, overwrite
])

# Initialize Spark with Iceberg
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database = args['database']
table_name = args['table_name']
fqtn = f"glue_catalog.{database}.{table_name}"
operation = args['operation']

# Read source data
source_df = spark.read.parquet(args['source_path'])
# Add processing metadata
source_df = source_df.withColumn("_ingested_at", current_timestamp())

# Write to Iceberg
if operation == 'append':
    source_df.writeTo(fqtn).append()

elif operation == 'upsert':
    source_df.createOrReplaceTempView("source_data")
    spark.sql(f"""
        MERGE INTO {fqtn} t
        USING source_data s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

elif operation == 'overwrite':
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    source_df.writeTo(fqtn).overwritePartitions()

job.commit()
```

**2. Table Setup Script (`setup_iceberg_table.py`)**:
```python
# Run once to create the Iceberg table
# Uses /iceberg-ddl skill output
```

**3. Maintenance Job (`glue_iceberg_maintenance.py`)**:
```python
# Uses /iceberg-maintenance skill output
# Scheduled to run daily via EventBridge
```

**4. Glue Job Configuration (CloudFormation)**:
```yaml
Resources:
  IcebergIngestionJob:
    Type: AWS::Glue::Job
    Properties:
      Name: !Sub '${ProducerName}-iceberg-ingestion'
      Role: !GetAtt GlueRole.Arn
      Command:
        Name: glueetl
        ScriptLocation: !Sub 's3://${ScriptBucket}/scripts/glue_iceberg_pipeline.py'
        PythonVersion: '3'
      DefaultArguments:
        '--datalake-formats': 'iceberg'
        '--enable-glue-datacatalog': 'true'
        '--database': !Ref Database
        '--table_name': !Ref TableName
        '--warehouse': !Sub 's3://${DataBucket}/warehouse/'
        '--conf': >-
          spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
          --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
          --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
          --conf spark.sql.catalog.glue_catalog.warehouse=s3://${DataBucket}/warehouse/
          --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
      GlueVersion: '4.0'
      NumberOfWorkers: 10
      WorkerType: G.1X

  MaintenanceJob:
    Type: AWS::Glue::Job
    Properties:
      Name: !Sub '${ProducerName}-iceberg-maintenance'
      Role: !GetAtt GlueRole.Arn
      Command:
        Name: glueetl
        ScriptLocation: !Sub 's3://${ScriptBucket}/scripts/glue_iceberg_maintenance.py'
        PythonVersion: '3'
      DefaultArguments:
        '--datalake-formats': 'iceberg'
        '--enable-glue-datacatalog': 'true'
      GlueVersion: '4.0'
      NumberOfWorkers: 5
      WorkerType: G.1X

  DailyMaintenanceTrigger:
    Type: AWS::Glue::Trigger
    Properties:
      Name: !Sub '${ProducerName}-daily-maintenance'
      Type: SCHEDULED
      Schedule: 'cron(0 2 * * ? *)'  # 2 AM daily
      Actions:
        - JobName: !Ref MaintenanceJob
          Arguments:
            '--database': !Ref Database
            '--table_name': !Ref TableName
            '--warehouse': !Sub 's3://${DataBucket}/warehouse/'

  GlueRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: glue.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
      Policies:
        - PolicyName: IcebergDataAccess
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - s3:GetObject
                  - s3:PutObject
                  - s3:DeleteObject
                  - s3:ListBucket
                Resource:
                  - !Sub 'arn:aws:s3:::${DataBucket}'
                  - !Sub 'arn:aws:s3:::${DataBucket}/*'
              - Effect: Allow
                Action:
                  - glue:GetTable
                  - glue:GetTables
                  - glue:UpdateTable
                  - glue:CreateTable
                  - glue:DeleteTable
                  - glue:GetDatabase
                  - glue:CreateDatabase
                Resource: '*'
              - Effect: Allow
                Action:
                  - lakeformation:GetDataAccess
                Resource: '*'
```

### 3B: New Producer - ECS/Lambda Python (PyIceberg)

**Lambda Handler (`iceberg_handler.py`)**:
```python
import json
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.glue import GlueCatalog
from datetime import datetime

def handler(event, context):
    config = {
        'warehouse': event['warehouse'],
        'region': event['region'],
        'database': event['database'],
        'table_name': event['table_name'],
        'source_path': event['source_path'],
        'operation': event.get('operation', 'append')
    }

    catalog = GlueCatalog("glue_catalog", **{
        "warehouse": config['warehouse'],
        "region_name": config['region']
    })

    table = catalog.load_table(f"{config['database']}.{config['table_name']}")

    # Read source data
    arrow_table = pq.read_table(config['source_path'])

    if config['operation'] == 'append':
        table.append(arrow_table)
    elif config['operation'] == 'overwrite':
        table.overwrite(arrow_table)
    elif config['operation'] == 'delete':
        from pyiceberg.expressions import EqualTo
        table.delete(delete_filter=EqualTo(event['delete_column'], event['delete_value']))

    return {
        'statusCode': 200,
        'body': json.dumps({
            'records_written': len(arrow_table),
            'snapshot_id': str(table.current_snapshot().snapshot_id)
        })
    }
```

### 3C: New Producer - ECS/Lambda Java

**Main Handler (`IcebergPipelineHandler.java`)**:
```java
package com.producer.iceberg;

import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import com.google.common.collect.ImmutableMap;

public class IcebergPipelineHandler {

    private final GlueCatalog catalog;

    public IcebergPipelineHandler(String warehouse, String region) {
        this.catalog = new GlueCatalog();
        this.catalog.initialize("glue_catalog", ImmutableMap.of(
            "warehouse", warehouse,
            "io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        ));
    }

    public void ingestData(String database, String tableName, List<GenericRecord> records) {
        Table table = catalog.loadTable(TableIdentifier.of(database, tableName));
        Schema schema = table.schema();

        // Write to Parquet data file
        String dataFilePath = table.locationProvider().newDataLocation(
            UUID.randomUUID() + ".parquet"
        );
        OutputFile outputFile = table.io().newOutputFile(dataFilePath);

        DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .build();

        try {
            for (GenericRecord record : records) {
                writer.write(record);
            }
        } finally {
            writer.close();
        }

        // Commit
        DataFile dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
    }
}
```

**Maven POM (`pom.xml`) - key dependencies**:
```xml
<dependencies>
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-core</artifactId>
        <version>1.7.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-aws</artifactId>
        <version>1.7.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-data</artifactId>
        <version>1.7.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-parquet</artifactId>
        <version>1.7.1</version>
    </dependency>
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>glue</artifactId>
        <version>2.25.0</version>
    </dependency>
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>s3</artifactId>
        <version>2.25.0</version>
    </dependency>
</dependencies>
```

### 3D: Existing Producer - Augmenting Existing Pipeline

For existing producers, analyze their current code and generate:

1. **Configuration additions**: Add Iceberg Spark/catalog configs to existing Spark session
2. **Write operation changes**: Replace existing Parquet writes with Iceberg writes
3. **Import additions**: Add necessary Iceberg imports
4. **Dependency additions**: Add Iceberg JARs/packages to build config
5. **Migration script**: One-time script to migrate existing data (use /iceberg-migrate)

**Before (existing Glue PySpark writing Parquet):**
```python
# EXISTING CODE - writes Parquet directly
df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/data/table/")
```

**After (modified for Iceberg):**
```python
# MODIFIED CODE - writes to Iceberg table
# Add Iceberg config to Spark session (at top of script)
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://bucket/warehouse/")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Replace Parquet write with Iceberg write
df.writeTo("glue_catalog.database.table").append()
# OR for partition overwrite:
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.writeTo("glue_catalog.database.table").overwritePartitions()
```

When analyzing existing code for augmentation:
1. Read the existing pipeline code
2. Identify the write operations (DataFrame.write, spark.sql INSERT, etc.)
3. Identify the Spark session creation
4. Generate minimal changes needed to switch to Iceberg
5. Preserve all existing transformation logic unchanged
6. Add Glue job parameter `--datalake-formats iceberg` if using Glue

### 3E: Existing Producer - Augmenting ECS/Lambda Python (PyIceberg)

**Before (existing Python writing Parquet directly via PyArrow/boto3):**
```python
# EXISTING CODE - writes Parquet directly to S3
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({"col1": [...], "col2": [...]})
pq.write_table(table, "s3://bucket/data/table/part-00001.parquet")
```

**After (modified for Iceberg via PyIceberg):**
```python
# MODIFIED CODE - writes to Iceberg table via PyIceberg
from pyiceberg.catalog.glue import GlueCatalog
import pyarrow as pa

catalog = GlueCatalog("glue_catalog", **{
    "warehouse": "s3://bucket/warehouse/",
    "region_name": "us-east-1"
})
iceberg_table = catalog.load_table("database.table")

# Same PyArrow table construction as before
arrow_table = pa.table({"col1": [...], "col2": [...]})

# Replace pq.write_table with Iceberg append
iceberg_table.append(arrow_table)
```

Add `pyiceberg[glue,s3fs,pyarrow]>=0.7.0` to requirements.txt.

### 3F: Existing Producer - Augmenting ECS/Lambda Java

**Before (existing Java writing Parquet directly):**
```java
// EXISTING CODE - writes Parquet directly to S3
ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
    .withSchema(avroSchema)
    .build();
for (GenericRecord record : records) { writer.write(record); }
writer.close();
// Then upload to S3 via AWS SDK
```

**After (modified for Iceberg):**
```java
// MODIFIED CODE - writes to Iceberg table
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;

GlueCatalog catalog = new GlueCatalog();
catalog.initialize("glue_catalog", ImmutableMap.of(
    "warehouse", "s3://bucket/warehouse/",
    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
));

Table table = catalog.loadTable(TableIdentifier.of("database", "table"));
OutputFile outputFile = table.io().newOutputFile(
    table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet")
);

// Same record construction as before, but write via Iceberg DataWriter
DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
    .schema(table.schema())
    .createWriterFunc(GenericParquetWriter::buildWriter)
    .overwrite()
    .build();
try {
    for (GenericRecord record : records) { writer.write(record); }
} finally { writer.close(); }

// Iceberg handles the commit (replaces manual S3 upload)
table.newAppend().appendFile(writer.toDataFile()).commit();
```

Add Iceberg Maven dependencies: `iceberg-core`, `iceberg-data`, `iceberg-parquet`, `iceberg-aws` (version 1.7.1).

## Step 4: Multi-Region Integration

If multi-region is needed, additionally generate:
1. S3 CRR configuration (use /iceberg-multi-region skill)
2. Metadata repointing Lambda
3. EventBridge trigger for sync
4. Monitoring for replication lag

## Step 5: Deliverable Checklist

Verify the generated pipeline includes:
- [ ] Table DDL (CREATE TABLE statement or API call)
- [ ] Spark/catalog configuration
- [ ] Ingestion code with error handling
- [ ] Maintenance job (compaction, snapshot expiry, orphan cleanup)
- [ ] IAM roles with least-privilege permissions
- [ ] Scheduling (EventBridge, Glue Trigger, or Step Functions)
- [ ] Monitoring (CloudWatch metrics, error alerts)
- [ ] Multi-region sync (if requested)
- [ ] Validation script (row counts, data quality)
- [ ] Dependency list (JAR versions, pip packages)
