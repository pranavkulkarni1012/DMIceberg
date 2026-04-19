---
description: "Generate end-to-end Iceberg pipeline for a producer. Use when a new producer needs a complete pipeline from scratch, or when an existing producer needs to add Iceberg support to their existing AWS Glue/EMR/ECS/Lambda pipeline."
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

**4. Glue Job Configuration (Terraform)**:

Notes on Glue + Iceberg configuration:
- `--datalake-formats = iceberg` installs the Iceberg runtime JARs onto the Glue worker; it does NOT auto-register a Spark catalog. You still need the `spark.sql.catalog.glue_catalog.*` configs. The canonical pattern ([AWS Glue Iceberg docs](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)) is: pass `--datalake-formats iceberg` as a job parameter AND set the `spark.sql.catalog.glue_catalog.*` keys -- either through `--conf` job parameters or in the job script via `SparkSession.builder` (shown above). Pick one place to set them so you have a single source of truth; the two forms are equivalent, not conflicting.
- Target **Glue 5.x** for new producers. Glue 5.0 (GA Dec 2024) launched with Iceberg 1.6.1 and was subsequently updated to 1.7.1 on Spark 3.5.4 / Python 3.11. Glue 5.1 (GA Nov 2025) ships Iceberg 1.10.0 on Spark 3.5.6 and adds Iceberg materialized-view + format-v3 support. Glue 4.0 still runs but is on the older Spark 3.3 runtime.
- If your buckets are encrypted with a customer-managed KMS key, grant the Glue role `kms:Decrypt` + `kms:GenerateDataKey` + `kms:Encrypt` on that key (see KMS block below).

```hcl
variable "producer_name"    { type = string }
variable "data_bucket"      { type = string }
variable "script_bucket"    { type = string }
variable "database"         { type = string }
variable "table_name"       { type = string }
variable "kms_key_arn"      { type = string  default = null }  # set if buckets use SSE-KMS

locals {
  warehouse = "s3://${var.data_bucket}/warehouse/"
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  glue_catalog_arn  = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog"
  glue_database_arn = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.database}"
  glue_table_arn    = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.database}/*"
}

resource "aws_iam_role" "glue" {
  name = "${var.producer_name}-iceberg-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "iceberg_access" {
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect = "Allow"
        # s3:DeleteObject is REQUIRED for compaction, snapshot expiry, orphan cleanup.
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetObjectVersion"]
        Resource = [
          "arn:aws:s3:::${var.data_bucket}",
          "arn:aws:s3:::${var.data_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable", "glue:GetTables", "glue:UpdateTable",
          "glue:CreateTable", "glue:DeleteTable",
          "glue:GetDatabase", "glue:CreateDatabase",
          "glue:GetPartitions", "glue:BatchCreatePartition"
        ]
        Resource = [local.glue_catalog_arn, local.glue_database_arn, local.glue_table_arn]
      },
      {
        # Lake Formation GetDataAccess must be "*"; the action does not support
        # resource-level scoping. Lake Formation enforces scope via its own grants.
        Effect = "Allow"
        Action = ["lakeformation:GetDataAccess"]
        Resource = "*"
      }
    ],
    var.kms_key_arn == null ? [] : [{
      Effect = "Allow"
      Action = ["kms:Decrypt", "kms:GenerateDataKey", "kms:Encrypt", "kms:DescribeKey"]
      Resource = var.kms_key_arn
    }])
  })
}

resource "aws_glue_job" "ingestion" {
  name     = "${var.producer_name}-iceberg-ingestion"
  role_arn = aws_iam_role.glue.arn
  glue_version      = "5.0"  # Glue 5.x ships Spark 3.5.4 + Iceberg; 5.1 bundles Iceberg 1.10.0.
  number_of_workers = 10
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${var.script_bucket}/scripts/glue_iceberg_pipeline.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"        = "iceberg"
    "--enable-glue-datacatalog" = "true"
    "--database"                = var.database
    "--table_name"              = var.table_name
    "--warehouse"               = local.warehouse
  }
}

resource "aws_glue_job" "maintenance" {
  name     = "${var.producer_name}-iceberg-maintenance"
  role_arn = aws_iam_role.glue.arn
  glue_version      = "5.0"  # Glue 5.x ships Spark 3.5.4 + Iceberg; 5.1 bundles Iceberg 1.10.0.
  number_of_workers = 5
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${var.script_bucket}/scripts/glue_iceberg_maintenance.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"        = "iceberg"
    "--enable-glue-datacatalog" = "true"
  }
}

resource "aws_glue_trigger" "daily_maintenance" {
  name     = "${var.producer_name}-daily-maintenance"
  type     = "SCHEDULED"
  schedule = "cron(0 2 * * ? *)"  # 2 AM UTC daily

  actions {
    job_name = aws_glue_job.maintenance.name
    arguments = {
      "--database"   = var.database
      "--table_name" = var.table_name
      "--warehouse"  = local.warehouse
    }
  }
}
```

### 3B: New Producer - ECS/Lambda Python (PyIceberg)

**Concurrency note**: Iceberg catalog commits are optimistic. If many Lambda invocations fire concurrently and all append to the same table, commits will contend and `CommitFailedException` will be raised. Wrap the table mutation in the `commit_with_retry` helper (Step 3G) OR funnel writes through a FIFO SQS queue for a single-writer pattern.

**Lambda Handler (`iceberg_handler.py`)**:
```python
import json
import os
import random
import time
import logging
import pyarrow.parquet as pq
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import CommitFailedException

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def commit_with_retry(fn, max_attempts=8, base_delay=0.25, max_delay=16.0):
    """
    Retry an Iceberg write operation on CommitFailedException.
    Uses exponential backoff with jitter so parallel Lambda invocations
    don't retry in lockstep.
    """
    attempt = 0
    while True:
        try:
            return fn()
        except CommitFailedException as e:
            attempt += 1
            if attempt >= max_attempts:
                raise
            delay = min(max_delay, base_delay * (2 ** attempt)) + random.uniform(0, 0.5)
            logger.warning("CommitFailedException on attempt %d: %s. Retrying in %.2fs", attempt, e, delay)
            time.sleep(delay)


def handler(event, context):
    config = {
        'warehouse':  event.get('warehouse')  or os.environ['WAREHOUSE'],
        'region':     event.get('region')     or os.environ['AWS_REGION'],
        'database':   event.get('database')   or os.environ['DATABASE'],
        'table_name': event.get('table_name') or os.environ['TABLE_NAME'],
        'source_path': event['source_path'],
        'operation':  event.get('operation', 'append'),
    }

    # Use PyIceberg's canonical config keys. glue.region + s3.region are authoritative
    # for the Glue catalog and the S3 FileIO; region_name is a boto3-passthrough and
    # may break on future PyIceberg versions.
    catalog = GlueCatalog("glue_catalog", **{
        "warehouse":  config['warehouse'],
        "glue.region": config['region'],
        "s3.region":   config['region'],
    })

    table = catalog.load_table(f"{config['database']}.{config['table_name']}")

    # Read source data into an Arrow table.
    arrow_table = pq.read_table(config['source_path'])

    # All mutations run through commit_with_retry so parallel Lambda invocations survive optimistic-concurrency contention.
    if config['operation'] == 'append':
        commit_with_retry(lambda: table.append(arrow_table))
    elif config['operation'] == 'overwrite':
        commit_with_retry(lambda: table.overwrite(arrow_table))
    elif config['operation'] == 'delete':
        from pyiceberg.expressions import EqualTo
        commit_with_retry(lambda: table.delete(
            delete_filter=EqualTo(event['delete_column'], event['delete_value'])
        ))
    else:
        raise ValueError(f"Unknown operation: {config['operation']}")

    # Reload to get the latest snapshot after our commit.
    table.refresh()
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
        // glue.region + s3.region must be set explicitly. Otherwise the AWS SDK
        // resolves region from env/IMDS/profile, which in a Lambda deployed in a
        // different region than the warehouse silently triggers cross-region S3
        // access -- explicitly disallowed on this platform.
        this.catalog.initialize("glue_catalog", ImmutableMap.of(
            "warehouse",   warehouse,
            "glue.region", region,
            "s3.region",   region,
            "io-impl",     "org.apache.iceberg.aws.s3.S3FileIO"
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

        // toDataFile() reads the finalized footer and must be called AFTER close().
        // Close explicitly (not in finally) so a flush failure surfaces before commit.
        // On any exception before commit, delete the orphan Parquet file so failed
        // runs don't leak S3 objects that no Iceberg snapshot references.
        DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .build();
        DataFile dataFile;
        try {
            for (GenericRecord record : records) {
                writer.write(record);
            }
            writer.close();
            dataFile = writer.toDataFile();
        } catch (Exception e) {
            try { writer.close(); } catch (Exception ignored) {}
            try { table.io().deleteFile(dataFilePath); } catch (Exception ignored) {}
            throw e;
        }
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
        <version>1.10.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-aws</artifactId>
        <version>1.10.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-data</artifactId>
        <version>1.10.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-parquet</artifactId>
        <version>1.10.1</version>
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
    "glue.region": "us-east-1",
    "s3.region":   "us-east-1",
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

// Same record construction as before, but write via Iceberg DataWriter.
// toDataFile() MUST be called AFTER close() succeeds (not inside a bare
// finally), and on failure we best-effort-delete the orphaned Parquet file.
DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
    .schema(table.schema())
    .createWriterFunc(GenericParquetWriter::buildWriter)
    .overwrite()
    .build();
DataFile dataFile;
try {
    for (GenericRecord record : records) { writer.write(record); }
    writer.close();
    dataFile = writer.toDataFile();
} catch (Exception e) {
    try { writer.close(); } catch (Exception ignored) {}
    try { table.io().deleteFile(outputFile.location()); } catch (Exception ignored) {}
    throw new RuntimeException("Iceberg write failed; orphan data file cleaned up", e);
}

// Iceberg handles the commit (replaces manual S3 upload)
table.newAppend().appendFile(dataFile).commit();
```

Add Iceberg Maven dependencies: `iceberg-core`, `iceberg-data`, `iceberg-parquet`, `iceberg-aws` (version 1.10.1).

### 3G: Cross-cutting patterns

**Commit retry (Java)** -- mandatory for any runtime where multiple writers may commit to the same table concurrently (parallel ECS tasks, Lambda fan-out):

```java
import org.apache.iceberg.exceptions.CommitFailedException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public final class IcebergCommits {
    private IcebergCommits() {}

    public static <T> T withRetry(java.util.function.Supplier<T> op) {
        int attempt = 0, max = 8;
        long baseMs = 250, capMs = 16_000;
        while (true) {
            try {
                return op.get();
            } catch (CommitFailedException e) {
                if (++attempt >= max) throw e;
                long delay = Math.min(capMs, baseMs * (1L << attempt))
                    + ThreadLocalRandom.current().nextLong(500);
                try { Thread.sleep(delay); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
    }
}

// Usage:
IcebergCommits.withRetry(() -> {
    table.newAppend().appendFile(dataFile).commit();
    return null;
});
```

**Writing to PARTITIONED tables (Java)** -- two distinct paths, and it matters which one you pick:

1. **Writer-produced files (normal ingestion)** -- when you construct a `DataWriter` for a partitioned table, pass the `PartitionKey` to the writer builder. `writer.toDataFile()` then carries the partition tuple automatically; you do NOT use `DataFiles.builder().withPartition(...)`:

    ```java
    import org.apache.iceberg.PartitionKey;
    import org.apache.iceberg.data.GenericRecord;

    // Compute the partition key from a representative record in this batch.
    PartitionKey pk = new PartitionKey(table.spec(), table.schema());
    pk.partition(sampleRecord);  // projects partition columns through the table's transforms

    DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
        .schema(table.schema())
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .withSpec(table.spec())           // REQUIRED for partitioned tables
        .withPartition(pk)                // REQUIRED: the writer records this on the DataFile
        .overwrite()
        .build();

    try { for (GenericRecord r : records) writer.write(r); } finally { writer.close(); }

    DataFile dataFile = writer.toDataFile();   // partition is already attached
    table.newAppend().appendFile(dataFile).commit();
    ```

    If a batch spans multiple partitions, partition the records in-memory first and open one writer per partition. The `PartitionedFanoutWriter` / `ClusteredDataWriter` helpers in `iceberg-data` automate this.

2. **Registering a pre-existing Parquet file (add-files / migration)** -- when you do NOT own the writer and only have a file path + statistics, use `DataFiles.builder()` and supply the partition yourself:

    ```java
    DataFile dataFile = DataFiles.builder(table.spec())
        .withPath(existingParquetPath)
        .withFileSizeInBytes(fileSize)
        .withRecordCount(recordCount)
        .withFormat(FileFormat.PARQUET)
        .withPartition(pk)                // REQUIRED when registering a raw file
        // .withMetrics(metrics)          // column-level stats for predicate pushdown
        .build();
    ```

**ECS (Fargate) deployment skeleton (Terraform)**:
```hcl
variable "producer_name" { type = string }
variable "data_bucket"   { type = string }
variable "database"      { type = string }
variable "image_uri"     { type = string }  # ECR image containing your ingestion code
variable "subnet_ids"    { type = list(string) }
variable "security_group_ids" { type = list(string) }
variable "kms_key_arn"   { type = string  default = null }

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  glue_catalog_arn  = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog"
  glue_database_arn = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.database}"
  glue_table_arn    = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.database}/*"
}

resource "aws_iam_role" "task" {
  name = "${var.producer_name}-iceberg-task-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "task" {
  role = aws_iam_role.task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetObjectVersion"]
        Resource = ["arn:aws:s3:::${var.data_bucket}", "arn:aws:s3:::${var.data_bucket}/*"]
      },
      {
        Effect = "Allow"
        Action = ["glue:GetTable","glue:GetTables","glue:UpdateTable","glue:CreateTable","glue:GetDatabase","glue:CreateDatabase"]
        Resource = [local.glue_catalog_arn, local.glue_database_arn, local.glue_table_arn]
      }
    ],
    var.kms_key_arn == null ? [] : [{
      Effect = "Allow"
      Action = ["kms:Decrypt","kms:GenerateDataKey","kms:Encrypt","kms:DescribeKey"]
      Resource = var.kms_key_arn
    }])
  })
}

resource "aws_ecs_cluster" "this" { name = "${var.producer_name}-iceberg" }

resource "aws_ecs_task_definition" "ingest" {
  family                   = "${var.producer_name}-iceberg-ingest"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.task.arn
  task_role_arn            = aws_iam_role.task.arn
  container_definitions = jsonencode([{
    name      = "ingest"
    image     = var.image_uri
    essential = true
    environment = [
      { name = "WAREHOUSE",  value = "s3://${var.data_bucket}/warehouse/" },
      { name = "AWS_REGION", value = data.aws_region.current.name }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = "/ecs/${var.producer_name}-iceberg-ingest"
        awslogs-region        = data.aws_region.current.name
        awslogs-stream-prefix = "ingest"
      }
    }
  }])
}

resource "aws_scheduler_schedule" "ingest" {
  name       = "${var.producer_name}-iceberg-ingest"
  group_name = "default"
  flexible_time_window { mode = "OFF" }
  schedule_expression = "rate(1 hour)"
  target {
    arn      = aws_ecs_cluster.this.arn
    role_arn = aws_iam_role.task.arn
    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.ingest.arn
      launch_type         = "FARGATE"
      network_configuration {
        subnets          = var.subnet_ids
        security_groups  = var.security_group_ids
        assign_public_ip = false
      }
    }
  }
}
```

**Lambda deployment skeleton (Terraform)**:
```hcl
variable "producer_name" { type = string }
variable "data_bucket"   { type = string }
variable "database"      { type = string }
variable "table_name"    { type = string }
variable "code_zip_path" { type = string }  # local path to packaged zip
variable "kms_key_arn"   { type = string  default = null }

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  glue_catalog_arn  = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog"
  glue_database_arn = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.database}"
  glue_table_arn    = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.database}/*"
}

resource "aws_iam_role" "lambda" {
  name = "${var.producer_name}-iceberg-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect = "Allow"
        Action = ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetObjectVersion"]
        Resource = ["arn:aws:s3:::${var.data_bucket}","arn:aws:s3:::${var.data_bucket}/*"]
      },
      {
        Effect = "Allow"
        Action = ["glue:GetTable","glue:GetTables","glue:UpdateTable","glue:CreateTable","glue:GetDatabase","glue:CreateDatabase"]
        Resource = [local.glue_catalog_arn, local.glue_database_arn, local.glue_table_arn]
      }
    ],
    var.kms_key_arn == null ? [] : [{
      Effect = "Allow"
      Action = ["kms:Decrypt","kms:GenerateDataKey","kms:Encrypt","kms:DescribeKey"]
      Resource = var.kms_key_arn
    }])
  })
}

resource "aws_lambda_function" "ingest" {
  function_name = "${var.producer_name}-iceberg-ingest"
  role          = aws_iam_role.lambda.arn
  handler       = "iceberg_handler.handler"
  runtime       = "python3.11"
  filename      = var.code_zip_path
  timeout       = 900   # 15 min max; consider ECS if ingestion exceeds this.
  memory_size   = 2048  # PyIceberg + PyArrow need room.
  environment {
    variables = {
      WAREHOUSE  = "s3://${var.data_bucket}/warehouse/"
      DATABASE   = var.database
      TABLE_NAME = var.table_name
    }
  }
  # Reserved concurrency = 1 enforces a single-writer pattern and avoids
  # CommitFailedException retries entirely. Remove if you rely on commit_with_retry.
  reserved_concurrent_executions = 1
}
```

**PyIceberg-producer maintenance (Glue PySpark) role** -- since PyIceberg cannot compact, Lambda/ECS-Python producers need a *separate* Glue job. Give that Glue role access to the Lambda producer's data bucket and table:

```hcl
# Reuse the producer's data_bucket and (optional) kms_key_arn variables.
resource "aws_iam_role" "maint_glue" {
  name = "${var.producer_name}-iceberg-maint-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "maint_glue_service" {
  role       = aws_iam_role.maint_glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "maint" {
  role = aws_iam_role.maint_glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect = "Allow"
        Action = ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetObjectVersion"]
        Resource = ["arn:aws:s3:::${var.data_bucket}","arn:aws:s3:::${var.data_bucket}/*"]
      },
      {
        Effect = "Allow"
        Action = ["glue:GetTable","glue:GetTables","glue:UpdateTable","glue:GetDatabase"]
        Resource = [local.glue_catalog_arn, local.glue_database_arn, local.glue_table_arn]
      }
    ],
    var.kms_key_arn == null ? [] : [{
      Effect = "Allow"
      Action = ["kms:Decrypt","kms:GenerateDataKey","kms:Encrypt","kms:DescribeKey"]
      Resource = var.kms_key_arn
    }])
  })
}
```

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
