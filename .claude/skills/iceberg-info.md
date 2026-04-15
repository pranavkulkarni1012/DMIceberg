---
description: "Inspect Iceberg table metadata: schema, snapshots, partitions, manifests, data files, properties, and history. Use when a producer needs to understand table state, debug issues, or audit table health."
user_invocable: true
---

# Iceberg Table Information & Inspection

## Goal
Generate code to inspect and query Iceberg table metadata, history, and health. Output code for the producer's specific tech stack.

## Step 1: Gather Requirements

Ask the producer (if not already provided):

1. **What to inspect**:
   - Table schema and properties
   - Snapshot history (commits)
   - Partition information
   - Data files (count, sizes, distribution)
   - Manifest files
   - Table health diagnostics (small files, delete file ratio)
   - Storage usage
2. **Tech stack**: Glue PySpark / EMR PySpark / ECS Python / Lambda Python / ECS Java / Lambda Java?
3. **Table**: database.table, S3 warehouse path, region

## Step 2: Generate Code

### For PySpark (Glue / EMR)

Iceberg exposes metadata through Spark metadata tables:

```python
# === TABLE SCHEMA & PROPERTIES ===
spark.sql("DESCRIBE TABLE EXTENDED glue_catalog.{database}.{table}").show(truncate=False)
spark.sql("SHOW TBLPROPERTIES glue_catalog.{database}.{table}").show(truncate=False)

# === SNAPSHOT HISTORY ===
# All snapshots with timestamps, operation, and summary
spark.sql("SELECT * FROM glue_catalog.{database}.{table}.snapshots").show(truncate=False)

# Commit history (snapshot log)
spark.sql("SELECT * FROM glue_catalog.{database}.{table}.history").show(truncate=False)

# Current snapshot details
spark.sql("""
    SELECT
        snapshot_id,
        committed_at,
        operation,
        summary['added-data-files'] as added_files,
        summary['added-records'] as added_records,
        summary['total-data-files'] as total_files,
        summary['total-records'] as total_records,
        summary['total-files-size'] as total_size_bytes
    FROM glue_catalog.{database}.{table}.snapshots
    ORDER BY committed_at DESC
    LIMIT 20
""").show(truncate=False)

# === PARTITION INFORMATION ===
spark.sql("SELECT * FROM glue_catalog.{database}.{table}.partitions").show(truncate=False)

# Partition statistics
spark.sql("""
    SELECT
        partition,
        record_count,
        file_count,
        spec_id
    FROM glue_catalog.{database}.{table}.partitions
    ORDER BY record_count DESC
""").show(truncate=False)

# === DATA FILES ===
spark.sql("""
    SELECT
        content,
        file_path,
        file_format,
        record_count,
        file_size_in_bytes,
        column_sizes,
        value_counts,
        null_value_counts
    FROM glue_catalog.{database}.{table}.files
""").show(truncate=False)

# === MANIFEST FILES ===
spark.sql("""
    SELECT
        path,
        length,
        partition_spec_id,
        added_snapshot_id,
        added_data_files_count,
        existing_data_files_count,
        deleted_data_files_count
    FROM glue_catalog.{database}.{table}.manifests
""").show(truncate=False)

# === ALL METADATA TABLES ===
# Available metadata tables:
# - {table}.history        : snapshot log
# - {table}.snapshots      : snapshot details with summaries
# - {table}.files          : current data files
# - {table}.manifests      : current manifest files
# - {table}.partitions     : partition statistics
# - {table}.all_data_files : data files across all snapshots
# - {table}.all_manifests  : manifests across all snapshots
# - {table}.all_entries    : manifest entries across all snapshots
# - {table}.refs           : branch and tag references
# - {table}.metadata_log_entries : metadata file history

# === TABLE HEALTH DIAGNOSTICS ===
# Small file analysis
spark.sql(f"""
    SELECT
        COUNT(*) as total_files,
        COUNT(CASE WHEN file_size_in_bytes < 67108864 THEN 1 END) as small_files_lt_64mb,
        COUNT(CASE WHEN file_size_in_bytes < 5242880 THEN 1 END) as tiny_files_lt_5mb,
        ROUND(AVG(file_size_in_bytes) / 1048576, 2) as avg_file_size_mb,
        ROUND(MIN(file_size_in_bytes) / 1048576, 2) as min_file_size_mb,
        ROUND(MAX(file_size_in_bytes) / 1048576, 2) as max_file_size_mb,
        ROUND(SUM(file_size_in_bytes) / 1073741824, 2) as total_size_gb,
        SUM(record_count) as total_records
    FROM glue_catalog.{database}.{table}.files
    WHERE content = 0
""").show(truncate=False)

# Delete file analysis (for merge-on-read tables)
spark.sql(f"""
    SELECT
        content,
        CASE content
            WHEN 0 THEN 'DATA'
            WHEN 1 THEN 'POSITION_DELETE'
            WHEN 2 THEN 'EQUALITY_DELETE'
        END as file_type,
        COUNT(*) as file_count,
        SUM(record_count) as total_records,
        ROUND(SUM(file_size_in_bytes) / 1048576, 2) as total_size_mb
    FROM glue_catalog.{database}.{table}.files
    GROUP BY content
""").show(truncate=False)

# Partition skew analysis
spark.sql(f"""
    SELECT
        partition,
        file_count,
        record_count,
        ROUND(record_count * 100.0 / SUM(record_count) OVER(), 2) as pct_of_total
    FROM glue_catalog.{database}.{table}.partitions
    ORDER BY record_count DESC
    LIMIT 20
""").show(truncate=False)
```

### For PyIceberg (ECS / Lambda Python)

```python
from pyiceberg.catalog.glue import GlueCatalog

catalog = GlueCatalog("glue_catalog", **{
    "warehouse": "s3://{bucket}/warehouse/",
    "region_name": "{region}"
})
table = catalog.load_table("{database}.{table}")

# === TABLE SCHEMA ===
print("Schema:")
print(table.schema())
print(f"\nFormat version: {table.metadata.format_version}")
print(f"\nPartition spec: {table.spec()}")
print(f"\nSort order: {table.sort_order()}")
print(f"\nLocation: {table.location()}")

# === TABLE PROPERTIES ===
print("\nProperties:")
for key, value in table.properties.items():
    print(f"  {key} = {value}")

# === SNAPSHOT HISTORY ===
print("\nSnapshots:")
for snapshot in table.metadata.snapshots:
    print(f"  ID: {snapshot.snapshot_id}")
    print(f"  Timestamp: {snapshot.timestamp_ms}")
    print(f"  Operation: {snapshot.summary.operation if snapshot.summary else 'N/A'}")
    if snapshot.summary:
        print(f"  Added files: {snapshot.summary.get('added-data-files', 'N/A')}")
        print(f"  Total records: {snapshot.summary.get('total-records', 'N/A')}")
    print()

# Current snapshot
current = table.current_snapshot()
if current:
    print(f"Current snapshot: {current.snapshot_id}")
    print(f"  Manifest list: {current.manifest_list}")

# === DATA FILES (via inspect) ===
data_files = table.inspect.data_files()
print(f"\nData files table: {data_files.num_rows} files")
print(data_files.to_pandas().describe())

# File size distribution
import pyarrow.compute as pc
sizes = data_files.column("file_size_in_bytes")
print(f"\nFile size stats:")
print(f"  Total files: {len(sizes)}")
print(f"  Avg size: {pc.mean(sizes).as_py() / 1048576:.2f} MB")
print(f"  Min size: {pc.min(sizes).as_py() / 1048576:.2f} MB")
print(f"  Max size: {pc.max(sizes).as_py() / 1048576:.2f} MB")
print(f"  Small files (<64MB): {pc.sum(pc.less(sizes, 67108864)).as_py()}")

# === PARTITION INFO ===
partitions = table.inspect.partitions()
print(f"\nPartitions: {partitions.num_rows}")
print(partitions.to_pandas())

# === MANIFESTS ===
manifests = table.inspect.manifests()
print(f"\nManifests: {manifests.num_rows}")
print(manifests.to_pandas())

# === METADATA LOG ===
print("\nMetadata log entries:")
for entry in table.metadata.metadata_log:
    print(f"  {entry.metadata_file} (timestamp: {entry.timestamp_ms})")

# === REFS (branches/tags) ===
print("\nRefs:")
for name, ref in table.metadata.refs.items():
    print(f"  {name}: snapshot_id={ref.snapshot_id}, type={ref.snapshot_ref_type}")
```

### For Java API (ECS / Lambda Java)

```java
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;

Table table = catalog.loadTable(TableIdentifier.of("{database}", "{table}"));

// === SCHEMA ===
System.out.println("Schema: " + table.schema());
System.out.println("Partition spec: " + table.spec());
System.out.println("Sort order: " + table.sortOrder());
System.out.println("Location: " + table.location());

// === PROPERTIES ===
table.properties().forEach((k, v) -> System.out.println("  " + k + " = " + v));

// === SNAPSHOTS ===
for (Snapshot snapshot : table.snapshots()) {
    System.out.println("Snapshot: " + snapshot.snapshotId());
    System.out.println("  Timestamp: " + snapshot.timestampMillis());
    System.out.println("  Operation: " + snapshot.operation());
    System.out.println("  Summary: " + snapshot.summary());
    System.out.println("  Manifest list: " + snapshot.manifestListLocation());
}

// Current snapshot
Snapshot current = table.currentSnapshot();
System.out.println("Current: " + current.snapshotId());
System.out.println("  Total records: " + current.summary().get("total-records"));
System.out.println("  Total files: " + current.summary().get("total-data-files"));

// === DATA FILES ===
TableScan scan = table.newScan();
long totalFiles = 0, totalSize = 0, totalRecords = 0;
long smallFiles = 0;
for (FileScanTask task : scan.planFiles()) {
    DataFile file = task.file();
    totalFiles++;
    totalSize += file.fileSizeInBytes();
    totalRecords += file.recordCount();
    if (file.fileSizeInBytes() < 67108864) smallFiles++;
}
System.out.println("Total files: " + totalFiles);
System.out.println("Total size: " + (totalSize / 1073741824) + " GB");
System.out.println("Total records: " + totalRecords);
System.out.println("Small files (<64MB): " + smallFiles);

// === MANIFESTS ===
for (ManifestFile manifest : current.allManifests(table.io())) {
    System.out.println("Manifest: " + manifest.path());
    System.out.println("  Added files: " + manifest.addedFilesCount());
    System.out.println("  Existing files: " + manifest.existingFilesCount());
    System.out.println("  Deleted files: " + manifest.deletedFilesCount());
}

// === METADATA LOG ===
// Access metadata log via TableOperations (internal API)
if (table instanceof HasTableOperations) {
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    for (TableMetadata.MetadataLogEntry entry : metadata.previousFiles()) {
        System.out.println("Metadata: " + entry.file() + " at " + entry.timestampMillis());
    }
}
```

## Step 3: Health Assessment Template

When the producer asks for a health check, generate a comprehensive diagnostic that covers:

1. **File health**: Small file count, average file size, size distribution
2. **Delete file overhead**: Ratio of delete files to data files (for MoR tables)
3. **Snapshot bloat**: Number of snapshots, age of oldest snapshot
4. **Partition skew**: Imbalanced partitions by record count
5. **Manifest efficiency**: Number of manifests, average entries per manifest
6. **Recommended actions** based on findings

Output a summary with actionable recommendations, e.g.:
- "43 small files detected (<64MB). Recommend running compaction."
- "127 snapshots found, oldest is 90 days. Recommend expiring snapshots older than 7 days."
- "High delete file ratio (35%). Recommend compaction to resolve merge-on-read overhead."
