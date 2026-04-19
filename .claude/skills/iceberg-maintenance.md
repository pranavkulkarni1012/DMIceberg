---
description: "Generate Iceberg table maintenance code: compaction, snapshot expiry, orphan file cleanup, manifest rewriting, and statistics collection. Use when a producer needs to optimize table performance or manage storage."
---

# Iceberg Table Maintenance

## Goal
Generate production-ready maintenance operation code for Iceberg tables. These operations are critical for table health, query performance, and storage cost management.

## Step 1: Gather Requirements

Ask the producer (if not already provided):

1. **Maintenance operation(s)** needed:
   - Small file compaction (rewrite data files)
   - Snapshot expiry
   - Orphan file cleanup
   - Manifest rewriting
   - Remove old metadata files
   - Rewrite position delete files (for merge-on-read tables)
2. **Tech stack**: Glue PySpark / EMR PySpark / ECS Python / Lambda Python / ECS Java / Lambda Java?
3. **Table**: database.table, S3 warehouse path, region
4. **Scheduling**: One-off or recurring? If recurring, what interval?
5. **Maintenance parameters** (or use defaults):
   - Compaction: target file size (default 128MB), partial progress enabled?
   - Snapshot expiry: retain last N snapshots or retain for N days?
   - Orphan cleanup: retention period (default 3 days)?

## Step 2: Maintenance Operations Reference

### Operation Priority & Scheduling Guide
| Operation | Priority | Recommended Schedule | Why |
|---|---|---|---|
| Snapshot expiry | HIGH | Daily or after every N commits | Prevents metadata bloat, reduces listing overhead |
| Small file compaction | HIGH | Daily or based on file count threshold | Prevents small file problem, improves read performance |
| Orphan file cleanup | MEDIUM | Weekly | Reclaims storage from failed commits |
| Manifest rewriting | MEDIUM | Weekly or after compaction | Optimizes manifest scan performance |
| Remove old metadata | LOW | Weekly | Cleans up old metadata.json files |
| Rewrite position deletes | HIGH (if MoR) | Daily for merge-on-read tables | Resolves delete files into base data |

### CRITICAL: Execution Order
Always run maintenance in this order:
1. **Expire snapshots** first (makes files eligible for cleanup)
2. **Compact data files** (merge small files, resolve deletes)
3. **Rewrite manifests** (optimize after compaction)
4. **Remove orphan files** (clean up after everything else)

Running orphan cleanup before snapshot expiry may delete files still referenced by unexpired snapshots.

## Step 3: Generate Code

### For PySpark (Glue / EMR)

Use Iceberg Spark Procedures (available via `spark.sql("CALL ...")`):

```python
# === 1. EXPIRE SNAPSHOTS ===
# Expire snapshots older than N days, keep minimum of 3
spark.sql(f"""
    CALL glue_catalog.system.expire_snapshots(
        table => '{database}.{table}',
        older_than => TIMESTAMP '{expire_before_timestamp}',
        retain_last => 3,
        max_concurrent_deletes => 10
    )
""")

# === 2. COMPACT DATA FILES (Rewrite Data Files) ===
# Bin-pack strategy (default) - merge small files
spark.sql(f"""
    CALL glue_catalog.system.rewrite_data_files(
        table => '{database}.{table}',
        options => map(
            'target-file-size-bytes', '134217728',
            'min-file-size-bytes', '67108864',
            'max-file-size-bytes', '201326592',
            'partial-progress.enabled', 'true',
            'partial-progress.max-commits', '10',
            'max-concurrent-file-group-rewrites', '5'
        )
    )
""")

# Sort-order compaction (for sorted tables)
spark.sql(f"""
    CALL glue_catalog.system.rewrite_data_files(
        table => '{database}.{table}',
        strategy => 'sort',
        sort_order => 'col1 ASC NULLS FIRST, col2 DESC',
        options => map(
            'target-file-size-bytes', '134217728',
            'partial-progress.enabled', 'true'
        )
    )
""")

# Z-order compaction (for multi-dimensional filtering)
spark.sql(f"""
    CALL glue_catalog.system.rewrite_data_files(
        table => '{database}.{table}',
        strategy => 'sort',
        sort_order => 'zorder(col1, col2)',
        options => map('target-file-size-bytes', '134217728')
    )
""")

# === 3. REWRITE MANIFESTS ===
spark.sql(f"""
    CALL glue_catalog.system.rewrite_manifests(
        table => '{database}.{table}'
    )
""")

# === 4. REMOVE ORPHAN FILES ===
# WARNING: Set older_than to at least 3 days to avoid removing files from in-progress commits
spark.sql(f"""
    CALL glue_catalog.system.remove_orphan_files(
        table => '{database}.{table}',
        older_than => TIMESTAMP '{orphan_before_timestamp}',
        dry_run => true
    )
""")
# Review dry_run output, then set dry_run => false

# === 5. REWRITE POSITION DELETE FILES (merge-on-read tables) ===
# Dedicated procedure for compacting position-delete files into fewer, larger files.
# Do NOT confuse with rewrite_data_files -- this operates on delete files, not data files.
spark.sql(f"""
    CALL glue_catalog.system.rewrite_position_delete_files(
        table => '{database}.{table}',
        options => map(
            'rewrite-all', 'false',
            'max-concurrent-file-group-rewrites', '5',
            'partial-progress.enabled', 'true'
        )
    )
""")

# After compacting position deletes, optionally also rewrite data files
# to resolve (apply) the deletes into new base files (removing the MoR overhead):
spark.sql(f"""
    CALL glue_catalog.system.rewrite_data_files(
        table => '{database}.{table}',
        options => map(
            'delete-file-threshold', '1',
            'target-file-size-bytes', '134217728',
            'partial-progress.enabled', 'true'
        )
    )
""")
```

**Complete Glue maintenance job template:**
```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database', 'table_name', 'warehouse'])

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

database = args['database']
table_name = args['table_name']
fqtn = f"{database}.{table_name}"

expire_ts = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
orphan_ts = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")

# 1. Expire snapshots
spark.sql(f"CALL glue_catalog.system.expire_snapshots(table => '{fqtn}', older_than => TIMESTAMP '{expire_ts}', retain_last => 3)")

# 2. (MoR tables only) Compact position delete files.
#    Probing format-version via snapshot summaries or SHOW TBLPROPERTIES is unreliable --
#    format-version is a metadata-json field, not a snapshot summary entry, and it's
#    only exposed as a tbl property when explicitly set. Instead, query the .files
#    metadata table directly: v1 tables cannot have delete files by spec (content=0 only),
#    so a positive count for content IN (1, 2) is sufficient and sound evidence that
#    rewrite_position_delete_files is worth running.
del_rows = spark.sql(f"""
    SELECT COUNT(*) AS n FROM glue_catalog.{fqtn}.files WHERE content IN (1, 2)
""").collect()
if del_rows and del_rows[0]['n'] > 0:
    spark.sql(f"CALL glue_catalog.system.rewrite_position_delete_files(table => '{fqtn}', options => map('partial-progress.enabled','true'))")

# 3. Compact data files
spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{fqtn}', options => map('target-file-size-bytes','134217728','partial-progress.enabled','true'))")

# 4. Rewrite manifests
spark.sql(f"CALL glue_catalog.system.rewrite_manifests(table => '{fqtn}')")

# 5. Remove orphan files
spark.sql(f"CALL glue_catalog.system.remove_orphan_files(table => '{fqtn}', older_than => TIMESTAMP '{orphan_ts}')")

spark.stop()
```

### For PyIceberg (ECS / Lambda Python)

PyIceberg has limited maintenance API. Some operations require Spark.

```python
from pyiceberg.catalog.glue import GlueCatalog
from datetime import datetime, timedelta

catalog = GlueCatalog("glue_catalog", **{
    "warehouse": "s3://{bucket}/warehouse/",
    "glue.region": "{region}",
    "s3.region":   "{region}",
})
table = catalog.load_table("{database}.{table}")

# EXPIRE SNAPSHOTS
expire_before = datetime.now() - timedelta(days=7)

# Optional: create a branch to preserve a reference before expiry
table.manage_snapshots() \
    .create_branch(table.current_snapshot().snapshot_id, "audit-backup") \
    .commit()

# Expire old snapshots
table.expire_snapshots() \
    .older_than(expire_before) \
    .commit()

# INSPECT for maintenance needs (to decide if compaction is needed)
files_df = table.inspect.data_files()
# Check file sizes, count small files
import pyarrow.compute as pc
file_sizes = files_df.column("file_size_in_bytes")
small_files = pc.filter(file_sizes, pc.less(file_sizes, 67108864))  # < 64MB
print(f"Small files count: {len(small_files)}")

# NOTE: The following operations are NOT available in PyIceberg as of 0.7.x:
# - Compaction (rewrite_data_files)
# - Manifest rewriting (rewrite_manifests)
# - Orphan file cleanup (remove_orphan_files)
#
# For these operations, producers using PyIceberg MUST use one of:
# 1. A separate AWS Glue PySpark job for maintenance (recommended)
# 2. The Iceberg Java API in an ECS container
#
# PyIceberg can only handle: snapshot expiry and metadata inspection.
```

**PyIceberg maintenance limitations:** Producers using PyIceberg who need compaction, manifest rewriting, or orphan file cleanup must:
- Create a separate Glue PySpark job for those operations (recommended, use the PySpark templates above)
- Or use the Iceberg Java API in an ECS container

### For Java API (ECS / Lambda Java)

```java
import org.apache.iceberg.*;
import org.apache.iceberg.actions.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.spark.actions.SparkActions;

Table table = catalog.loadTable(TableIdentifier.of("{database}", "{table}"));

// === 1. EXPIRE SNAPSHOTS ===
// Prefer the Actions API when Spark is available -- it both expires snapshots and
// deletes the files (data / manifests / manifest-lists) that were referenced
// ONLY by those expired snapshots, in parallel. NOTE: this does NOT remove true
// orphans -- files on S3 that no live or expired metadata ever referenced, e.g.,
// partial writes from a failed commit. Run DeleteOrphanFiles separately for those
// (step 5 below).
long expireTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
SparkActions.get()
    .expireSnapshots(table)
    .expireOlderThan(expireTimestamp)
    .retainLast(3)
    .execute();

// Standalone Java (no Spark runtime available): the core table API still works,
// but file deletion is serial. For large tables, run this via an ECS task with
// adequate timeout rather than a Lambda.
//   long expireTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
//   table.expireSnapshots()
//       .expireOlderThan(expireTimestamp)
//       .retainLast(3)
//       .cleanExpiredFiles(true)
//       .commit();

// === 2. COMPACT DATA FILES ===
// Using Iceberg Actions (requires Spark runtime for full rewrite)
// For standalone Java, use the table API for manifest operations:
RewriteFiles rewrite = table.newRewrite();
// ... manual file-level rewrite logic ...
// For production compaction, recommend using Spark Actions:
// SparkActions.get().rewriteDataFiles(table)
//     .option("target-file-size-bytes", "134217728")
//     .option("partial-progress.enabled", "true")
//     .execute();

// === 3. REWRITE MANIFESTS ===
RewriteManifests rewriteManifests = table.rewriteManifests();
for (ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
    if (manifest.addedFilesCount() + manifest.existingFilesCount() < 100) {
        rewriteManifests.rewriteIf(m -> m.path().equals(manifest.path()));
    }
}
rewriteManifests.commit();

// === 4. REMOVE ORPHAN FILES ===
// Requires listing S3 and comparing against metadata
Set<String> knownFiles = new HashSet<>();
for (Snapshot snapshot : table.snapshots()) {
    for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        // collect all known data file paths
    }
}
// Compare with S3 listing, delete unknowns older than 3 days
```

## Step 4: Scheduling Templates

### EventBridge + Glue Workflow (recommended for Glue users)
```json
{
    "schedule": "rate(1 day)",
    "target": "glue-workflow",
    "workflow": "{producer}-iceberg-maintenance"
}
```

### EventBridge + Lambda (for serverless maintenance)
```python
# Lambda handler for maintenance scheduling
def handler(event, context):
    glue_client = boto3.client('glue')
    glue_client.start_job_run(
        JobName=f"{event['database']}-iceberg-maintenance",
        Arguments={
            '--database': event['database'],
            '--table_name': event['table_name'],
            '--warehouse': event['warehouse']
        }
    )
```

### Step Functions (for orchestrated maintenance)
Generate a Step Functions state machine that runs maintenance operations in the correct order with error handling between steps.

## Step 5: Validate Output

1. Verify maintenance operations are in the correct execution order
2. Verify retention periods are safe (orphan cleanup >= 3 days)
3. Verify dry_run is used for orphan cleanup on first run
4. Include error handling and logging
5. Include scheduling configuration if recurring
