# DMIceberg - Data Mesh Iceberg Migration Platform

## Project Purpose

This project provides Claude Code skills and subagents to help Data Mesh producers migrate from Parquet table format to Apache Iceberg table format, with multi-region resilience capabilities.

## Architecture Context

### Data Mesh Model
- Each producer owns their own S3 bucket and pipeline
- Producers store data in Parquet format today
- Producers use diverse tech stacks (see below)
- Each producer is responsible for their own table lifecycle

### Producer Tech Stacks
| Stack | Runtime | Language | Iceberg Library |
|-------|---------|----------|-----------------|
| AWS Glue | PySpark | Python | iceberg-spark-runtime |
| AWS EMR | PySpark | Python | iceberg-spark-runtime |
| AWS ECS | Container | Python | pyiceberg |
| AWS ECS | Container | Java | iceberg-core + iceberg-aws |
| AWS Lambda | Serverless | Python | pyiceberg |
| AWS Lambda | Serverless | Java | iceberg-core + iceberg-aws |

### Multi-Region Constraints (CRITICAL)
- Cross-region S3 access is **NOT allowed**
- Multi-Region Access Points are **NOT allowed**
- Each region must have its own S3 bucket with local data copies
- S3 Cross-Region Replication (CRR) is used to replicate data
- Iceberg metadata must be **repointed** to reference the local region's S3 paths
- Tables must be registered in each region's Glue Catalog independently

### Iceberg Catalog
- All producers use **AWS Glue Data Catalog** as the Iceberg catalog
- Warehouse path convention: `s3://<producer-bucket>/warehouse/`
- Database naming: producer-managed

## Skills (Slash Commands)

| Skill | Purpose |
|-------|---------|
| `/iceberg-onboard` | **Primary entry point.** End-to-end onboarding for new or existing producers |
| `/iceberg-ddl` | Create, alter, drop Iceberg tables with schema evolution |
| `/iceberg-data` | Insert, upsert, delete data in Iceberg tables |
| `/iceberg-maintenance` | Compaction, snapshot expiry, orphan cleanup |
| `/iceberg-info` | Table metadata inspection and diagnostics |
| `/iceberg-migrate` | Migrate existing Parquet/Hive tables to Iceberg |
| `/iceberg-multi-region` | Setup multi-region resilience with metadata repointing |
| `/iceberg-pipeline` | Generate end-to-end pipeline for new or existing producers |

## Subagents

| Agent | Purpose |
|-------|---------|
| `iceberg-orchestrator` | **Coordinates end-to-end onboarding.** Delegates to specialized agents below |
| `iceberg-architect` | Analyze producer setup and design migration strategy |
| `iceberg-code-generator` | Generate platform-specific production code |
| `iceberg-validator` | Validate configurations, schemas, and generated code |
| `iceberg-multi-region-planner` | Plan and generate multi-region infrastructure and sync |

## Key Conventions

### Spark Session Configuration (Glue/EMR)
```python
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://<bucket>/warehouse/")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
```

### PyIceberg Catalog (ECS/Lambda Python)
```python
from pyiceberg.catalog.glue import GlueCatalog
catalog = GlueCatalog("glue_catalog", **{"warehouse": "s3://<bucket>/warehouse/"})
```

### Java Catalog (ECS/Lambda Java)
```java
GlueCatalog catalog = new GlueCatalog();
catalog.initialize("glue_catalog", ImmutableMap.of(
    "warehouse", "s3://<bucket>/warehouse/",
    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
));
```

### Iceberg Format Version
- Default to **format-version 2** (supports row-level deletes, equality deletes)
- Use merge-on-read for upsert-heavy workloads
- Use copy-on-write for read-heavy workloads

### Multi-Region Metadata Repointing Flow
1. Source region writes Iceberg data + metadata to source S3 bucket
2. S3 CRR replicates all objects to target region bucket
3. Repointing utility reads replicated metadata.json, rewrites all S3 paths from source to target bucket
4. Manifest-list and manifest Avro files are rewritten with updated paths
5. Repointed metadata is written to target bucket
6. Table is registered in target region's Glue Catalog pointing to repointed metadata
