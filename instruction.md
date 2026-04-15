# DMIceberg Platform - Usage Instructions

Instructions for using the Data Mesh Iceberg Migration Platform to migrate existing producers from Parquet to Iceberg, or to onboard new producers directly onto Iceberg.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Quick Reference: Skills & Agents](#quick-reference)
4. [For Existing Producers (Parquet to Iceberg Migration)](#existing-producers)
5. [For New Producers (Greenfield Iceberg Setup)](#new-producers)
6. [Multi-Region Resilience (Both Producer Types)](#multi-region)
7. [Ongoing Operations](#ongoing-operations)
8. [Skill Details](#skill-details)
9. [Subagent Details](#subagent-details)
10. [Supported Tech Stacks](#supported-tech-stacks)
11. [Troubleshooting & FAQ](#troubleshooting)

---

## 1. Overview <a name="overview"></a>

DMIceberg provides 8 slash-command skills and 5 specialized subagents within Claude Code to help Data Mesh producers:

- **Onboard** producers end-to-end with a single command (`/iceberg-onboard`)
- **Migrate** existing Parquet/Hive tables to Apache Iceberg
- **Create** new Iceberg tables and pipelines from scratch
- **Manage** ongoing table maintenance (compaction, snapshot expiry, cleanup)
- **Inspect** table health, metadata, and diagnostics
- **Enable** multi-region resilience with S3 replication and metadata repointing

**Fastest path:** Run `/iceberg-onboard` -- it coordinates the entire journey (assessment, code generation, validation, multi-region) into a single deliverable. Use the individual skills when you need a targeted, single operation.

All generated code targets **AWS Glue Data Catalog** as the Iceberg catalog, **format-version 2** by default, and supports six different producer tech stacks.

---

## 2. Prerequisites <a name="prerequisites"></a>

Before using any skill, ensure the producer has:

### AWS Infrastructure
- An S3 bucket for the Iceberg warehouse (convention: `s3://<producer-bucket>/warehouse/`)
- IAM roles with permissions for S3 (GetObject, PutObject, DeleteObject, ListBucket) and Glue Catalog (GetTable, UpdateTable, CreateTable, GetDatabase, CreateDatabase)
- AWS Glue Data Catalog enabled in the target region

### For Glue PySpark / EMR PySpark Producers
- AWS Glue 4.0 or EMR 6.5+/7.x
- Glue jobs must include `--datalake-formats iceberg` in job parameters

### For ECS/Lambda Python Producers
- `pyiceberg[glue,s3fs,pyarrow]>=0.7.0` installed

### For ECS/Lambda Java Producers
- Iceberg JARs: `iceberg-core`, `iceberg-aws`, `iceberg-data`, `iceberg-parquet` (version 1.7.1)
- AWS SDK v2: `s3`, `glue` (version 2.25.0)

### For Multi-Region
- S3 versioning enabled on both source and target buckets
- Target region S3 bucket created
- IAM role for S3 Cross-Region Replication
- `fastavro` Python package (for metadata repointing utility)

---

## 3. Quick Reference: Skills & Agents <a name="quick-reference"></a>

### Skills (Slash Commands)

| Skill | When to Use |
|---|---|
| `/iceberg-onboard` | **Start here.** End-to-end onboarding for new or existing producers |
| `/iceberg-pipeline` | Generate a complete pipeline (alternative to onboard for targeted use) |
| `/iceberg-ddl` | Create, alter, or drop Iceberg tables |
| `/iceberg-data` | Insert, upsert, delete, or overwrite data in Iceberg tables |
| `/iceberg-migrate` | Migrate existing Parquet/Hive data to Iceberg format |
| `/iceberg-maintenance` | Set up compaction, snapshot expiry, orphan cleanup |
| `/iceberg-info` | Inspect table metadata, health, and diagnostics |
| `/iceberg-multi-region` | Set up multi-region resilience with S3 CRR and metadata repointing |

### Subagents (Used Internally by Skills)

| Agent | What It Does |
|---|---|
| `iceberg-orchestrator` | Coordinates end-to-end onboarding; delegates to all agents below |
| `iceberg-architect` | Analyzes a producer's codebase, infrastructure, and data to design a migration strategy |
| `iceberg-code-generator` | Generates production-ready code for any supported tech stack |
| `iceberg-validator` | Validates configurations, schemas, code, and multi-region setups |
| `iceberg-multi-region-planner` | Designs complete multi-region infrastructure and failover plans |

---

## 4. For Existing Producers (Parquet to Iceberg Migration) <a name="existing-producers"></a>

Use this workflow when a producer already has a running pipeline that writes Parquet data and wants to convert to Iceberg.

### Fastest Path: `/iceberg-onboard`

Run `/iceberg-onboard` and tell it you're an existing producer. It will coordinate everything below automatically -- assessment, migration code, pipeline changes, maintenance, validation, and multi-region (if needed) -- into a single deliverable. Use the manual steps below only if you prefer step-by-step control.

### Step 1: Assess the Current State

**Recommended approach: Start a conversation with Claude Code and let the `iceberg-architect` subagent analyze the producer's codebase.**

Provide Claude Code with:
- The producer's codebase location or repository
- Which tables need migration
- Whether multi-region resilience is needed

The architect subagent will automatically:
- Identify the tech stack (Glue, EMR, ECS, Lambda, Python, Java)
- Map current write operations and data format
- Assess data volume, schema, and partitioning
- Evaluate downstream consumer compatibility
- Produce a risk assessment and recommended strategy

Alternatively, use `/iceberg-pipeline` with the producer type set to "existing" to get a guided assessment.

### Step 2: Choose a Migration Strategy

Four migration strategies are available. The choice depends on the current state:

| Strategy | Data Copy? | Downtime | Best For | Requires |
|---|---|---|---|---|
| **In-place migrate** | No | Minutes | Hive tables already in Glue Catalog | PySpark (Glue/EMR) |
| **CTAS** | Yes | Hours (size-dependent) | Any source; cleanest result | Any tech stack |
| **Snapshot** | No | Seconds | Read-only testing before full migration | PySpark (Glue/EMR) |
| **Add-files** | No | Minutes | Raw Parquet on S3 with no catalog entry | PySpark (Glue/EMR) |

**Decision guide:**
- Producer has a **Hive table in Glue Catalog** and can pause writes briefly --> **In-place migrate**
- Producer has **raw Parquet files on S3** (no catalog entry) --> **Add-files** or **CTAS**
- Producer has a **large table** and needs zero downtime --> **CTAS with dual-write cutover**
- Producer wants to **test Iceberg** before committing --> **Snapshot** first, then CTAS
- Producer uses **PyIceberg or Java** (not PySpark) --> **CTAS** or **Add-files** (in-place and snapshot are Spark-only procedures)

**Important:** In-place migrate and snapshot strategies use Iceberg Spark Procedures and are only available in PySpark. PyIceberg/Java producers who need these strategies should use a one-time Glue PySpark job for the migration step.

### Step 3: Migrate the Data

Run `/iceberg-migrate` and provide:
1. Current table location and format
2. Current schema (or let it be inferred from Parquet files)
3. Current partitioning scheme
4. Chosen migration strategy
5. Target table details (database, table name, warehouse path, region)
6. Whether the table is actively written to during migration

The skill generates:
- Complete migration code for the chosen strategy and tech stack
- Post-migration validation script (row counts, schema, checksums)
- Cutover plan with rollback procedure

### Step 4: Modify the Existing Pipeline

Run `/iceberg-pipeline` with producer type "existing" to generate the minimal code changes needed:

**What changes in the existing pipeline:**

| Component | Before (Parquet) | After (Iceberg) |
|---|---|---|
| Spark config | No Iceberg config | 5 Iceberg config lines added |
| Write operation | `df.write.parquet("s3://...")` | `df.writeTo("glue_catalog.db.table").append()` |
| Imports | Standard Spark | Same (no new imports for PySpark) |
| Glue parameters | No `--datalake-formats` | Add `--datalake-formats iceberg` |

For PySpark producers, the changes are minimal:
```python
# ADD: Iceberg Spark config (at top of script, after SparkSession creation)
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://<bucket>/warehouse/")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# REPLACE: Parquet write with Iceberg write
# Before: df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/data/table/")
# After:
df.writeTo("glue_catalog.database.table").append()
```

For PyIceberg producers (ECS/Lambda Python):
```python
# ADD: PyIceberg catalog initialization
from pyiceberg.catalog.glue import GlueCatalog
catalog = GlueCatalog("glue_catalog", **{"warehouse": "s3://<bucket>/warehouse/", "region_name": "<region>"})
iceberg_table = catalog.load_table("database.table")

# REPLACE: Direct Parquet write with Iceberg append
# Before: pq.write_table(table, "s3://bucket/data/table/part-00001.parquet")
# After:
iceberg_table.append(arrow_table)
```

### Step 5: Set Up Maintenance

Run `/iceberg-maintenance` to generate a maintenance job. This is critical -- without maintenance, Iceberg tables accumulate small files, stale snapshots, and orphan files over time.

The skill generates:
- A maintenance job (Glue/EMR/Lambda) that runs operations in the correct order:
  1. Expire snapshots (7-day default retention, keep last 3)
  2. Compact data files (128MB target file size)
  3. Rewrite manifests
  4. Remove orphan files (3-day minimum retention)
- Scheduling configuration (EventBridge, Glue Trigger, or Step Functions)
- Recommended: daily maintenance schedule

### Step 6: Validate

Run `/iceberg-info` to inspect the migrated table and verify:
- Schema matches the original
- Row counts match
- Partitions are correctly mapped
- No small file issues
- Snapshot history shows the migration commit

### Complete Workflow Summary for Existing Producers

**Option A: Single command (recommended)**
```
/iceberg-onboard (type: existing)      --> Handles everything end-to-end
```

**Option B: Manual step-by-step**
```
1. /iceberg-pipeline (type: existing)  --> Assess current state, get augmentation plan
   OR let iceberg-architect subagent    --> Deep codebase analysis + migration strategy
       analyze the codebase

2. /iceberg-migrate                    --> Migrate existing Parquet data to Iceberg

3. /iceberg-pipeline (type: existing)  --> Generate minimal pipeline code changes

4. /iceberg-maintenance                --> Set up ongoing table maintenance

5. /iceberg-info                       --> Validate the migration

6. /iceberg-multi-region (if needed)   --> Set up multi-region resilience
```

---

## 5. For New Producers (Greenfield Iceberg Setup) <a name="new-producers"></a>

Use this workflow when a producer is starting fresh with no existing data or pipeline.

### Fastest Path: `/iceberg-onboard`

Run `/iceberg-onboard` and tell it you're a new producer. It will generate everything -- table DDL, ingestion pipeline, maintenance job, IAM roles, scheduling, monitoring, dependencies, and multi-region (if needed) -- as a single validated package. Use the manual steps below only if you prefer step-by-step control.

### Step 1: Generate the Complete Pipeline

Run `/iceberg-pipeline` with producer type "new" and provide:
1. **Tech stack**: Glue PySpark, EMR PySpark, ECS Python, Lambda Python, ECS Java, or Lambda Java
2. **Data source**: Where data comes from (S3, JDBC, API, Kafka, Kinesis, SQS)
3. **Data characteristics**: Volume, schema, update pattern (append/upsert/full refresh)
4. **Partitioning strategy**: By date, category, or ask for recommendations
5. **Target table**: Database name, table name, S3 bucket, region
6. **Multi-region**: Whether multi-region resilience is needed
7. **Scheduling**: How often the pipeline runs

The skill generates a complete pipeline including:

| Deliverable | Description |
|---|---|
| Table DDL | CREATE TABLE statement with schema, partitioning, and properties |
| Spark/catalog config | Full Iceberg configuration for the chosen tech stack |
| Ingestion code | Complete pipeline script with error handling |
| Maintenance job | Compaction, snapshot expiry, orphan cleanup |
| IAM roles | Least-privilege IAM policies (CloudFormation) |
| Scheduling | EventBridge/Glue Trigger/Step Functions configuration |
| Monitoring | CloudWatch metrics and error alerting |
| Dependency list | JAR versions, pip packages, or Maven coordinates |

### Step 2: Create the Iceberg Table

If you need to customize the table beyond what `/iceberg-pipeline` generated, use `/iceberg-ddl`:

Provide:
- Column definitions (name, type, nullable, comment)
- Partition strategy (identity, bucket, truncate, year/month/day/hour transforms)
- Sort order (optional)
- Table properties (format-version, write mode, compression)

The skill generates table creation code for your tech stack with:
- Full type mapping reference (Spark SQL, PyIceberg, Java)
- Partition transform reference
- Table property recommendations

**Partition strategy guidance:**
- **Date-based queries** (most common): `PARTITIONED BY (days(event_time))` or `PARTITIONED BY (months(event_time))`
- **High-cardinality lookups**: `PARTITIONED BY (bucket(16, user_id))`
- **String prefix queries**: `PARTITIONED BY (truncate(4, region_code))`
- **Multiple dimensions**: Combine transforms, e.g., `PARTITIONED BY (days(event_time), bucket(8, user_id))`

### Step 3: Implement Data Ingestion

If you need to customize data operations beyond what `/iceberg-pipeline` generated, use `/iceberg-data`:

Supported operations:
- **INSERT (append)**: Add new records to the table
- **UPSERT (MERGE INTO)**: Update existing records or insert new ones based on a merge key
- **DELETE**: Remove records matching a condition
- **OVERWRITE PARTITION**: Replace all data in specific partitions

**Write mode guidance:**
- Use **copy-on-write** (`'write.delete.mode'='copy-on-write'`) for read-heavy workloads (fewer, larger reads; slower writes)
- Use **merge-on-read** (`'write.delete.mode'='merge-on-read'`) for upsert-heavy workloads (faster writes; requires compaction)

### Step 4: Set Up Maintenance

Run `/iceberg-maintenance` (same as existing producers -- this step is equally critical for new producers).

### Step 5: Verify Table Health

Run `/iceberg-info` to confirm:
- Table was created with the expected schema and properties
- Initial data load completed successfully
- File sizes are in the healthy range (64MB-256MB)
- Partitioning is working as expected

### Complete Workflow Summary for New Producers

**Option A: Single command (recommended)**
```
/iceberg-onboard (type: new)           --> Handles everything end-to-end
```

**Option B: Manual step-by-step**
```
1. /iceberg-pipeline (type: new)       --> Generate complete end-to-end pipeline
                                            (table DDL + ingestion + maintenance + IAM + scheduling)

2. /iceberg-ddl (if customization      --> Customize table schema, partitioning, or properties
   needed)

3. /iceberg-data (if customization     --> Customize data operations (upsert logic, streaming, etc.)
   needed)

4. /iceberg-maintenance                --> Verify/customize maintenance schedule

5. /iceberg-info                       --> Validate table creation and initial data load

6. /iceberg-multi-region (if needed)   --> Set up multi-region resilience
```

---

## 6. Multi-Region Resilience (Both Producer Types) <a name="multi-region"></a>

Multi-region setup is the same whether the producer is new or existing. Run this after the base Iceberg pipeline is working.

### Critical Constraints

- **Cross-region S3 access is NOT allowed** -- workloads in region B cannot read S3 in region A
- **Multi-Region Access Points are NOT allowed**
- Each region must have its own S3 bucket with a **complete local copy** of data and metadata
- Each region has its own **independent Glue Data Catalog**
- Iceberg metadata files contain absolute S3 paths that must be **rewritten** for the target region

### Architecture

```
Source Region (e.g., us-east-1)          Target Region (e.g., us-west-2)
+---------------------------+            +---------------------------+
| Glue Catalog              |            | Glue Catalog              |
|   db.table (source)       |            |   db.table (replica)      |
|          |                |            |          |                |
| s3://source-bucket/       |  S3 CRR   | s3://target-bucket/       |
|   warehouse/              | ---------> |   warehouse/              |
|     data/ + metadata/     |            |     data/ (replicated)    |
+---------------------------+            |     metadata/ (REPOINTED) |
                                         +---------------------------+
                                                    ^
                                           Repointing Utility rewrites
                                           all S3 paths in metadata from
                                           source-bucket -> target-bucket
```

### How to Set Up

Run `/iceberg-multi-region` and provide:
1. Source region and S3 bucket
2. Target region(s) and S3 bucket(s)
3. Tables to replicate
4. Tech stack for the repointing utility (Python or Java)
5. Sync frequency (after every commit, hourly, daily)
6. Infrastructure provisioning tool (CloudFormation, CDK, Terraform)

The skill generates:

| Component | Description |
|---|---|
| S3 CRR CloudFormation | Replication role, bucket config, warehouse prefix filtering |
| Metadata repointing utility | Python or Java class that rewrites all 3 metadata layers |
| Glue Catalog registration | Code to register the table in the target region's Glue Catalog |
| Sync automation | EventBridge + Lambda trigger (event-driven or scheduled) |
| Monitoring & validation | Replication lag checks, metadata freshness, row count comparison |
| Failover runbook | Pre-failover checklist, failover steps, post-failover validation, failback |

### Metadata Repointing: What It Does

Iceberg metadata has three layers, all containing absolute S3 paths:

| Layer | File Type | Contains References To |
|---|---|---|
| `metadata.json` | JSON | Manifest-list file paths |
| `snap-*.avro` (manifest-list) | Avro | Manifest file paths |
| `*-m*.avro` (manifest) | Avro | Data file paths |

The repointing utility reads each layer from the target bucket (where S3 CRR replicated it), replaces all `s3://source-bucket/` references with `s3://target-bucket/`, and writes the repointed versions back. The Glue Catalog in the target region is then updated to point to the repointed metadata.

### Supported Multi-Region Patterns

| Pattern | Use Case | Writes |
|---|---|---|
| **Active-Passive (DR)** | Disaster recovery failover | Source only; target is standby |
| **Active-Active Reads** | Cross-region read replicas | Source writes; target reads only |

**Warning:** True active-active writes (both regions writing to the same logical table) are NOT recommended. Iceberg has no built-in conflict resolution for concurrent writes across regions.

---

## 7. Ongoing Operations <a name="ongoing-operations"></a>

### Table Maintenance

Run `/iceberg-maintenance` periodically or set up automated scheduling.

**Operations and recommended schedule:**

| Operation | Priority | Schedule | Why |
|---|---|---|---|
| Snapshot expiry | HIGH | Daily | Prevents metadata bloat |
| Small file compaction | HIGH | Daily | Prevents small file problem, improves reads |
| Orphan file cleanup | MEDIUM | Weekly | Reclaims storage from failed commits |
| Manifest rewriting | MEDIUM | Weekly or after compaction | Optimizes manifest scan performance |
| Position delete rewrite | HIGH (MoR only) | Daily for merge-on-read tables | Resolves delete files into base data |

**Critical execution order** (always run in this sequence):
1. Expire snapshots (makes files eligible for cleanup)
2. Compact data files (merge small files, resolve deletes)
3. Rewrite manifests (optimize after compaction)
4. Remove orphan files (clean up last)

**PyIceberg limitation:** PyIceberg (as of 0.7.x) only supports snapshot expiry. For compaction, manifest rewriting, and orphan cleanup, PyIceberg/Lambda producers must use a separate Glue PySpark maintenance job.

### Table Inspection & Health Checks

Run `/iceberg-info` to diagnose issues:

- **Small files accumulating** --> Run compaction
- **Too many snapshots** --> Run snapshot expiry
- **High delete file ratio** (merge-on-read tables) --> Run compaction to resolve deletes
- **Partition skew** --> Consider repartitioning with `/iceberg-ddl` ALTER
- **Storage growing unexpectedly** --> Check for orphan files; run cleanup

### Schema Evolution

Use `/iceberg-ddl` with the ALTER operation to:
- Add new columns (safe, non-breaking)
- Rename columns (safe with Iceberg's field ID tracking)
- Widen types (e.g., INT to BIGINT -- safe)
- Drop columns (breaking for consumers reading that column)
- Change partition strategy (Iceberg supports partition evolution without rewriting data)

---

## 8. Skill Details <a name="skill-details"></a>

### `/iceberg-onboard`
**Best starting point for all users.** End-to-end onboarding via the orchestrator agent.

- **Input**: Producer type (new/existing), codebase path (existing), tech stack, table details, schema, partitioning, multi-region needs
- **What happens**: Gathers requirements, then delegates to the `iceberg-orchestrator` agent which coordinates assessment (architect), code generation (code-generator), validation (validator), and multi-region planning (multi-region-planner) into a single deliverable
- **Output**: Complete deployment-ready package with all artifacts, deployment sequence, validation results, and rollback plan
- **When NOT to use**: For targeted single operations (just a DDL, just a MERGE INTO, just a health check) -- use the individual skills instead

### `/iceberg-pipeline`
Generates a complete end-to-end pipeline. Use when you want manual step-by-step control instead of `/iceberg-onboard`.

- **Input**: Producer type (new/existing), tech stack, data source, schema, partitioning, target table, scheduling, multi-region needs
- **Output**: Table DDL, ingestion code, maintenance job, IAM policies, scheduling config, monitoring, dependency list
- **For existing producers**: Analyzes current code and generates minimal changes to switch from Parquet to Iceberg
- **For new producers**: Generates a complete pipeline from scratch

### `/iceberg-ddl`
Create, alter, or drop Iceberg tables with schema evolution.

- **Input**: Operation (CREATE/ALTER/DROP), tech stack, table details, column definitions, partition strategy
- **Output**: DDL code for the chosen tech stack, type mapping reference, partition transform reference
- **Includes**: Iceberg type mapping table across all three APIs (Spark SQL, PyIceberg, Java)

### `/iceberg-data`
Generate data ingestion and manipulation code.

- **Input**: Operation (INSERT/UPSERT/DELETE/OVERWRITE), tech stack, source data details, target table, merge keys (for upsert)
- **Output**: Complete data operation code with error handling
- **Supports**: Batch and streaming ingestion, conditional merge, dynamic partition overwrite

### `/iceberg-migrate`
Migrate existing Parquet/Hive tables to Iceberg.

- **Input**: Current state (location, schema, partitioning, size), migration strategy, target table details
- **Output**: Migration code, post-migration validation script, cutover plan with rollback
- **Four strategies**: In-place migrate, CTAS, snapshot, add-files

### `/iceberg-maintenance`
Set up table maintenance operations.

- **Input**: Operations needed, tech stack, table details, scheduling preferences, tuning parameters
- **Output**: Complete maintenance job with correct execution order, scheduling configuration
- **Includes**: Compaction strategies (bin-pack, sort, z-order), scheduling templates (EventBridge, Glue Trigger, Step Functions)

### `/iceberg-info`
Inspect table metadata and health.

- **Input**: What to inspect (schema, snapshots, partitions, data files, health), tech stack, table details
- **Output**: Diagnostic code for the chosen tech stack
- **Health assessment covers**: Small file count, delete file ratio, snapshot bloat, partition skew, manifest efficiency

### `/iceberg-multi-region`
Set up multi-region resilience.

- **Input**: Source/target regions and buckets, tables, repointing tech stack, sync frequency, infrastructure tool
- **Output**: S3 CRR config, metadata repointing utility (Python or Java), Glue Catalog registration, sync automation, monitoring, failover runbook

---

## 9. Subagent Details <a name="subagent-details"></a>

Subagents are specialized agents used internally by skills or invoked directly by Claude Code when deeper analysis is needed.

### `iceberg-orchestrator`
**When it runs:** When `/iceberg-onboard` is invoked, or when a user requests complete end-to-end onboarding.

**What it does:**
1. **Phase 1 - ASSESS**: For existing producers, spawns `iceberg-architect` to analyze the codebase. For new producers, validates requirements.
2. **Phase 2 - PLAN**: Creates an artifact checklist (migration script, pipeline code, maintenance, IAM, multi-region) and presents it for user approval.
3. **Phase 3 - GENERATE**: Spawns `iceberg-code-generator` for pipeline artifacts and `iceberg-multi-region-planner` if multi-region is needed. Runs independent generation in parallel.
4. **Phase 4 - VALIDATE**: Spawns `iceberg-validator` on all generated code and configuration. Fixes any CRITICAL/HIGH issues.
5. **Phase 5 - DELIVER**: Packages everything into a deployment-ready deliverable with ordered deployment steps, rollback plan, and post-deployment checklist.

**Key principle:** Delegates domain knowledge to specialized agents. It coordinates and synthesizes, it doesn't duplicate their logic.

### `iceberg-architect`
**When it runs:** When a producer needs a migration strategy, or when `/iceberg-pipeline` is used for an existing producer.

**What it does:**
1. Explores the producer's codebase to identify tech stack, write operations, schema, partitioning, and data volumes
2. Maps the data flow (source, transformations, frequency, write pattern, downstream consumers)
3. Assesses infrastructure (IAM roles, S3 config, scheduling, IaC stacks)
4. Evaluates risks (data, schema, downtime, compatibility, performance, cost)
5. Checks downstream consumer compatibility (Athena v2/v3, Redshift Spectrum, EMR, Glue, Presto/Trino)
6. Recommends migration approach, partitioning, format version, write mode, and maintenance plan
7. Produces a structured migration plan with rollback procedure

### `iceberg-code-generator`
**When it runs:** When any skill needs to produce code for a specific tech stack.

**Standards it follows:**
- Production-ready (error handling, logging, retries)
- Configurable (environment variables, no hardcoded values)
- Idempotent (safe to retry)
- Includes full dependency declarations
- Includes IAM policy requirements
- Validates output (correct imports, API usage, error handling, no hardcoded credentials)

### `iceberg-validator`
**When it runs:** When generated code or configurations need verification.

**What it validates:**
- Schema (type compatibility, field IDs, partition column types)
- Spark configuration (all 5 required config keys, common mistakes)
- PyIceberg configuration (warehouse, region, catalog type)
- Java configuration (io-impl, catalog initialization)
- Table properties (format-version, write mode, file size targets)
- Code correctness (catalog prefix in SQL, merge keys, writer cleanup)
- Multi-region (CRR config, versioning, repointing completeness, path replacement)
- Maintenance (execution order, retention periods, scheduling)
- IAM/Security (S3 permissions, Glue permissions, no overly broad wildcards)

**Severity levels:** CRITICAL (runtime failure), HIGH (data/performance issues), MEDIUM (best practice violation), LOW (style/optimization)

### `iceberg-multi-region-planner`
**When it runs:** When `/iceberg-multi-region` is invoked.

**What it produces:**
- Architecture design (Active-Passive or Active-Active Reads pattern)
- Infrastructure components (S3 buckets, replication, IAM, Lambda)
- Metadata repointing strategy (full three-layer rewrite)
- Sync automation design (event-driven, scheduled, or Step Functions)
- Monitoring and alerting (replication lag, metadata freshness, data consistency)
- Failover runbook (pre-checks, failover steps, post-validation, failback)
- Cost estimation
- Implementation sequence

---

## 10. Supported Tech Stacks <a name="supported-tech-stacks"></a>

| Stack | Runtime | Language | Iceberg Library | Catalog Setup |
|---|---|---|---|---|
| AWS Glue | PySpark | Python | iceberg-spark-runtime | Spark config + `--datalake-formats iceberg` |
| AWS EMR | PySpark | Python | iceberg-spark-runtime | Spark config + iceberg JAR in spark-submit |
| AWS ECS | Container | Python | pyiceberg | `GlueCatalog("glue_catalog", ...)` |
| AWS Lambda | Serverless | Python | pyiceberg | `GlueCatalog("glue_catalog", ...)` |
| AWS ECS | Container | Java | iceberg-core + iceberg-aws | `GlueCatalog.initialize(...)` |
| AWS Lambda | Serverless | Java | iceberg-core + iceberg-aws | `GlueCatalog.initialize(...)` |

### Feature Availability by Tech Stack

| Feature | PySpark (Glue/EMR) | PyIceberg (ECS/Lambda Python) | Java (ECS/Lambda Java) |
|---|---|---|---|
| CREATE TABLE | Yes | Yes | Yes |
| ALTER TABLE | Yes | Yes | Yes |
| DROP TABLE | Yes | Yes | Yes |
| INSERT (append) | Yes | Yes | Yes |
| UPSERT (MERGE INTO) | Yes (SQL) | Limited (read-modify-write) | Yes (RowDelta API) |
| DELETE | Yes | Yes | Yes |
| OVERWRITE PARTITION | Yes | Yes | Yes |
| In-place migrate | Yes (Spark procedure) | No | No |
| Snapshot migrate | Yes (Spark procedure) | No | No |
| CTAS migrate | Yes | Yes | Yes |
| Add-files migrate | Yes (Spark procedure) | Yes (manual) | Yes (manual) |
| Compaction | Yes (Spark procedure) | No | Limited |
| Snapshot expiry | Yes | Yes | Yes |
| Orphan cleanup | Yes (Spark procedure) | No | Manual |
| Manifest rewrite | Yes (Spark procedure) | No | Limited |
| Streaming ingestion | Yes (Structured Streaming) | No | No |
| Metadata tables | Yes (Spark SQL) | Yes (inspect API) | Yes (Java API) |

---

## 11. Troubleshooting & FAQ <a name="troubleshooting"></a>

### Common Issues

**"Table not found" after migration**
- Verify the table is registered in Glue Catalog with `table_type = ICEBERG` and a valid `metadata_location`
- Ensure the catalog prefix is used in SQL: `glue_catalog.database.table`, not just `database.table`

**Slow read performance**
- Run `/iceberg-info` to check for small files (< 64MB)
- Run `/iceberg-maintenance` to compact data files to ~128MB

**"CommitFailedException" during writes**
- This is a normal optimistic concurrency conflict when multiple writers target the same table
- Implement retry logic with exponential backoff (the code generator includes this)

**Maintenance job deleting files still in use**
- Always run operations in the correct order: expire snapshots first, orphan cleanup last
- Orphan cleanup retention must be >= 3 days
- Snapshot expiry should retain at least 1 snapshot (3 recommended)

**Multi-region: target region shows stale data**
- Check S3 CRR replication status and lag (CloudWatch S3 replication metrics)
- Verify the repointing Lambda is running (check CloudWatch Logs)
- Run the validation script from `/iceberg-multi-region` to compare snapshot IDs between regions

**PyIceberg producers need compaction**
- PyIceberg does not support compaction natively
- Create a separate AWS Glue PySpark job for maintenance (the maintenance skill generates this)

**Glue job fails with "Iceberg not found" errors**
- Ensure `--datalake-formats iceberg` is in the Glue job parameters
- Ensure Glue version is 4.0

**Wrong file IO (HadoopFileIO instead of S3FileIO)**
- Always set `spark.sql.catalog.glue_catalog.io-impl` to `org.apache.iceberg.aws.s3.S3FileIO`
- Missing this config defaults to HadoopFileIO which does not work well with S3

### When to Use Which Skill

| Scenario | Skill to Use |
|---|---|
| "I'm a new producer, set everything up for me" | `/iceberg-onboard` (type: new) |
| "I have an existing Parquet pipeline, help me switch to Iceberg" | `/iceberg-onboard` (type: existing) |
| "I want step-by-step control over my pipeline setup" | `/iceberg-pipeline` |
| "I just need to create a table" | `/iceberg-ddl` |
| "I need to add upsert logic to my pipeline" | `/iceberg-data` |
| "My table has too many small files" | `/iceberg-maintenance` (compaction) |
| "Is my table healthy?" | `/iceberg-info` |
| "I need disaster recovery across regions" | `/iceberg-multi-region` |
| "I want a full migration plan with risk assessment" | `/iceberg-onboard` or let the `iceberg-architect` subagent analyze your codebase |
| "I need to add a column to my table" | `/iceberg-ddl` (ALTER) |
| "My multi-region replica is out of sync" | `/iceberg-multi-region` (monitoring/validation section) |

### Downstream Consumer Compatibility

Before migrating to Iceberg format-version 2, verify that all downstream consumers support it:

| Engine | Iceberg v1 | Iceberg v2 (row-level deletes) | Notes |
|---|---|---|---|
| Athena v3 | Yes | Yes | Full support |
| Athena v2 | Yes | No | v1 only; no delete file support |
| Redshift Spectrum | Yes | Yes (read) | Check latest feature availability |
| EMR 6.5+ / 7.x | Yes | Yes | Full support via Spark runtime |
| Glue 4.0 | Yes | Yes | Requires `--datalake-formats` |
| Presto/Trino | Yes | Varies | Depends on connector version |

If any consumer uses an engine that doesn't support format-version 2, either stay on v1 or upgrade the consumer first.
