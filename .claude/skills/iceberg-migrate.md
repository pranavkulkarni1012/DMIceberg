---
description: "Migrate existing Parquet or Hive tables to Iceberg format. Use when a producer has existing data in Parquet/Hive and wants to convert to Iceberg, including in-place migration, snapshot-based migration, or CTAS approaches."
---

# Parquet/Hive to Iceberg Migration

## Goal
Generate a complete migration plan and code to convert existing Parquet or Hive-format tables to Apache Iceberg, preserving data and minimizing downtime. Output code for the producer's specific tech stack.

## Step 1: Gather Requirements

Ask the producer (if not already provided):

1. **Current state**:
   - Is the table registered in Glue Catalog as a Hive table, or is it raw Parquet on S3?
   - Current S3 location of data
   - Current schema (or can we infer from Parquet files?)
   - Current partitioning (Hive-style `col=value/` directories?)
   - Approximate data size and file count
   - Is the table actively being written to during migration?
2. **Tech stack**: Glue PySpark / EMR PySpark / ECS Python / Lambda Python / ECS Java / Lambda Java?
3. **Migration strategy preference**:
   - **In-place migration** (fastest, reuses data files, no data copy)
   - **CTAS migration** (creates new table, copies data, cleanest)
   - **Snapshot migration** (read-only Iceberg view over existing data)
   - **Add-files** (register existing Parquet files without moving them)
4. **Target table details**: database, table name, S3 warehouse path, region
5. **Downtime tolerance**: Can writes be paused during migration?
6. **Schema evolution needs**: Any schema changes during migration?
7. **Post-migration**: Keep old table as backup? Rename?

### Schema inference from existing Parquet files

When the producer says "infer from data" (or when there's no Hive catalog entry to read), derive the Iceberg schema from a representative Parquet file in S3 before generating DDL. Do NOT hand-translate schemas from source docs — Parquet's type system (INT96 timestamps, logical types, decimal precision) is the authoritative source and the only way to guarantee Iceberg types line up with what the data files actually contain.

**PySpark (Glue/EMR):**

```python
# Read one Parquet file (or a small partition) and print the inferred Spark schema.
# Use this output to hand-write the Iceberg CREATE TABLE column list.
sample_df = spark.read.parquet("s3://{source_bucket}/{source_prefix}/").limit(0)
print(sample_df.schema.treeString())
# For a ready-to-paste DDL, use CTAS -- Spark will translate the schema for you:
#   CREATE TABLE glue_catalog.{db}.{tbl} USING iceberg AS SELECT * FROM parquet.`s3://.../` LIMIT 0
```

Spark auto-maps Parquet INT96 timestamps to Spark `TIMESTAMP`, Parquet `DECIMAL(p,s)` to Spark `DECIMAL(p,s)`, etc. When you then write via Iceberg, these map to `timestamp`, `decimal(p,s)` in Iceberg — no manual conversion needed. However, for **date-partitioned source data**, check whether the source used Hive-style `date=YYYY-MM-DD/` directory names — Spark reads those as strings unless you set `spark.sql.sources.partitionColumnTypeInference.enabled=true` (it is by default). You may need to cast to `date` explicitly if the strings don't parse.

**PyIceberg (ECS/Lambda Python):**

```python
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import pyarrow_to_schema

# PyArrow reads the Parquet footer schema without scanning any row groups -- cheap even for huge files.
arrow_schema = pq.read_schema("s3://{source_bucket}/{source_prefix}/part-00000.parquet")
iceberg_schema = pyarrow_to_schema(arrow_schema)
print(iceberg_schema)
# Pass `iceberg_schema` directly into catalog.create_table(...) -- no manual translation needed.
```

**Common gotchas:**
- Parquet files written by different writers (Spark vs Pandas vs AWS Firehose) may have slightly different nullability or logical-type annotations for the same logical column. Sample a file from each writer before locking in the schema.
- If the source table has evolved (columns added over time), `pq.read_schema` on only the latest file will miss dropped-and-readded columns. Scan the footer of the oldest and newest partition and union.
- `INT96` timestamps (legacy Hive / old Impala output) will surface as Iceberg `timestamp` without timezone. If your data is actually UTC, declare the Iceberg column as `timestamptz` and rely on Parquet's millisecond/microsecond adjustment on read.

## Step 2: Migration Strategy Decision Matrix

| Strategy | Data Copy? | Downtime | Best For | Risk |
|---|---|---|---|---|
| **In-place (migrate)** | No | Minutes | Hive tables in Glue Catalog | Modifies existing catalog entry |
| **CTAS** | Yes | Hours (depends on size) | Any source, cleanest result | Doubles storage temporarily |
| **Snapshot** | No | Seconds | Read-only use cases, testing | Source data must not change |
| **Add-files** | No | Minutes | Raw Parquet on S3, no catalog | Manual schema mapping |

### Tech Stack Availability
| Strategy | PySpark (Glue/EMR) | PyIceberg (ECS/Lambda Python) | Java (ECS/Lambda Java) |
|---|---|---|---|
| **In-place migrate** | YES (Spark procedure) | NO - Spark only | NO - Spark only |
| **CTAS** | YES | YES (read Parquet + append) | YES (read + write DataFiles) |
| **Snapshot** | YES (Spark procedure) | NO - Spark only | NO - Spark only |
| **Add-files** | YES (Spark procedure) | YES (read Parquet + append) | YES (build DataFiles manually) |

**Important:** In-place migrate and snapshot strategies use Iceberg Spark Procedures (`system.migrate`, `system.snapshot`) which are ONLY available in Spark. PyIceberg and Java producers must use CTAS or add-files strategies. If a PyIceberg/Java producer needs in-place or snapshot migration, recommend a one-time Glue PySpark job for the migration step.

### Strategy Recommendations
- **Hive table in Glue Catalog + can afford brief pause** -> In-place migrate
- **Raw Parquet on S3** -> Add-files or CTAS
- **Large table + need zero downtime** -> CTAS with dual-write cutover
- **Testing/validation before full migration** -> Snapshot first, then CTAS
- **Active writes during migration** -> CTAS with change data capture

## Step 3: Generate Code

### For PySpark (Glue / EMR)

**Strategy 1: In-Place Migration (Hive -> Iceberg)**
```python
# Prerequisites:
# - Table must exist in Glue Catalog as a Hive/Parquet table
# - Spark session configured with Iceberg extensions

# Migrate the table in-place
spark.sql(f"""
    CALL glue_catalog.system.migrate(
        table => '{database}.{table}'
    )
""")

# Verify migration
spark.sql(f"DESCRIBE EXTENDED glue_catalog.{database}.{table}").show(truncate=False)
spark.sql(f"SELECT COUNT(*) as row_count FROM glue_catalog.{database}.{table}").show()

# The table is now Iceberg. Original data files are referenced, not copied.
# Metadata files are created in the table's location.
```

**Strategy 2: CTAS (Create Table As Select)**
```python
# Read from any source
source_df = spark.read.parquet("s3://{source_bucket}/{source_path}")
# OR
source_df = spark.table("glue_catalog.{database}.{source_table}")

# Validate source data
print(f"Source row count: {source_df.count()}")
print(f"Source schema:")
source_df.printSchema()

# Create Iceberg table via CTAS
source_df.createOrReplaceTempView("migration_source")

spark.sql(f"""
    CREATE TABLE glue_catalog.{database}.{target_table}
    USING iceberg
    PARTITIONED BY ({partition_spec})
    LOCATION 's3://{target_bucket}/warehouse/{database}/{target_table}'
    TBLPROPERTIES (
        'format-version' = '2',
        'write.distribution-mode' = 'hash',
        'write.parquet.compression-codec' = 'zstd'
    )
    AS SELECT * FROM migration_source
""")

# Validate migration
target_count = spark.sql(f"SELECT COUNT(*) FROM glue_catalog.{database}.{target_table}").collect()[0][0]
source_count = source_df.count()
assert target_count == source_count, f"Row count mismatch: source={source_count}, target={target_count}"

print(f"Migration validated: {target_count} rows")
```

**Strategy 3: Snapshot Migration (read-only Iceberg overlay)**
```python
# Create an Iceberg table that references existing data files without copying
spark.sql(f"""
    CALL glue_catalog.system.snapshot(
        source_table => '{database}.{source_hive_table}',
        table => '{database}.{target_iceberg_table}'
    )
""")
# Source Hive table remains unchanged
# Target Iceberg table references the same data files
```

**Strategy 4: Add Files (register existing Parquet files)**
```python
# First create the Iceberg table with matching schema
spark.sql(f"""
    CREATE TABLE glue_catalog.{database}.{table} (
        {column_definitions}
    )
    USING iceberg
    PARTITIONED BY ({partition_spec})
    LOCATION 's3://{bucket}/warehouse/{database}/{table}'
    TBLPROPERTIES ('format-version' = '2')
""")

# Add existing Parquet files to the table
# For Hive-partitioned data (col=value/ directories):
spark.sql(f"""
    CALL glue_catalog.system.add_files(
        table => '{database}.{table}',
        source_table => '`parquet`.`s3://{source_bucket}/{source_path}`',
        partition_filter => map('year', '2024')
    )
""")

# For non-partitioned data:
spark.sql(f"""
    CALL glue_catalog.system.add_files(
        table => '{database}.{table}',
        source_table => '`parquet`.`s3://{source_bucket}/{source_path}`'
    )
""")
```

**Full Migration Job Template (Glue)**:
```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'database', 'source_table', 'target_table',
    'source_path', 'warehouse', 'strategy', 'partition_spec'
])

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

strategy = args['strategy']
database = args['database']
source_table = args['source_table']
target_table = args['target_table']

if strategy == 'migrate':
    # In-place
    spark.sql(f"CALL glue_catalog.system.migrate(table => '{database}.{source_table}')")
elif strategy == 'snapshot':
    spark.sql(f"CALL glue_catalog.system.snapshot(source_table => '{database}.{source_table}', table => '{database}.{target_table}')")
elif strategy == 'ctas':
    source_df = spark.read.parquet(args['source_path'])
    source_count = source_df.count()
    source_df.createOrReplaceTempView("src")
    spark.sql(f"""
        CREATE TABLE glue_catalog.{database}.{target_table}
        USING iceberg
        PARTITIONED BY ({args['partition_spec']})
        LOCATION '{args["warehouse"]}/{database}/{target_table}'
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
        AS SELECT * FROM src
    """)
    target_count = spark.sql(f"SELECT COUNT(*) FROM glue_catalog.{database}.{target_table}").collect()[0][0]
    assert source_count == target_count, f"VALIDATION FAILED: {source_count} vs {target_count}"
elif strategy == 'add_files':
    spark.sql(f"""
        CALL glue_catalog.system.add_files(
            table => '{database}.{target_table}',
            source_table => '`parquet`.`{args["source_path"]}`'
        )
    """)

# Post-migration validation
spark.sql(f"SELECT * FROM glue_catalog.{database}.{target_table}.snapshots").show(truncate=False)
spark.stop()
```

### For PyIceberg (ECS / Lambda Python)

```python
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType
import pyarrow.parquet as pq
import pyarrow as pa

catalog = GlueCatalog("glue_catalog", **{
    "warehouse": "s3://{bucket}/warehouse/",
    "glue.region": "{region}",
    "s3.region":   "{region}",
})

# Step 1: Infer schema from existing Parquet files
parquet_schema = pq.read_schema("s3://{source_bucket}/{source_path}/part-00000.parquet")
print(f"Source Parquet schema:\n{parquet_schema}")

# Step 2: Create Iceberg table with matching schema
# PyIceberg can create from PyArrow schema
table = catalog.create_table(
    identifier="{database}.{table}",
    schema=parquet_schema,  # PyIceberg accepts PyArrow schemas
    properties={"format-version": "2"}
)

# Step 3: Read source Parquet and append to Iceberg
# For smaller datasets (fits in memory):
arrow_table = pq.read_table("s3://{source_bucket}/{source_path}")
table.append(arrow_table)

# For larger datasets, process in batches:
import s3fs
fs = s3fs.S3FileSystem()
parquet_files = fs.glob(f"{source_bucket}/{source_path}/**/*.parquet")

for batch_start in range(0, len(parquet_files), 100):
    batch_files = parquet_files[batch_start:batch_start + 100]
    tables = [pq.read_table(f"s3://{f}") for f in batch_files]
    combined = pa.concat_tables(tables)
    table.append(combined)
    print(f"Migrated batch {batch_start // 100 + 1}: {len(combined)} rows")

# Step 4: Validate
scan = table.scan()
result = scan.to_arrow()
print(f"Target Iceberg row count: {len(result)}")
```

### For Java API (ECS / Lambda Java)

```java
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;

// Use Iceberg's migrate action (if Spark Actions available)
// For standalone Java, manual file registration:

GlueCatalog catalog = new GlueCatalog();
catalog.initialize("glue_catalog", ImmutableMap.of(
    "warehouse", "s3://{bucket}/warehouse/",
    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
));

// Step 1: Read Parquet schema and create Iceberg table
// (Parse schema from Parquet footer)
Schema icebergSchema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get())
    // ... map from Parquet schema
);

TableIdentifier tableId = TableIdentifier.of("{database}", "{table}");
Table table = catalog.createTable(tableId, icebergSchema,
    PartitionSpec.builderFor(icebergSchema).day("event_date").build(),
    "s3://{bucket}/warehouse/{database}/{table}",
    ImmutableMap.of("format-version", "2"));

// Step 2: Register existing Parquet files
AppendFiles append = table.newAppend();
// For each Parquet file in source:
for (String parquetPath : sourceParquetPaths) {
    DataFile dataFile = DataFiles.builder(table.spec())
        .withPath(parquetPath)
        .withFileSizeInBytes(fileSize)
        .withRecordCount(recordCount)
        .withFormat(FileFormat.PARQUET)
        .withPartitionPath("event_date=2024-01-15")  // if partitioned
        .build();
    append.appendFile(dataFile);
}
append.commit();

// Step 3: Set name mapping for schema evolution compatibility
NameMapping nameMapping = MappingUtil.create(table.schema());
table.updateProperties()
    .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping))
    .commit();
```

## Step 4: Post-Migration Validation Checklist

Generate a validation script that checks:

1. **Row count match**: Source vs target record count
2. **Schema compatibility**: All source columns present in target
3. **Sample data comparison**: Random sample of rows match between source and target
4. **Partition mapping**: All source partitions are represented in target
5. **Null integrity**: Null counts match for each column
6. **Aggregation check**: SUM/MIN/MAX of numeric columns match

```python
# Validation template (PySpark)
def validate_migration(spark, source_path, target_table):
    source_df = spark.read.parquet(source_path)
    target_df = spark.table(f"glue_catalog.{target_table}")

    # Row count
    s_count = source_df.count()
    t_count = target_df.count()
    assert s_count == t_count, f"Row count: {s_count} vs {t_count}"

    # Schema
    s_cols = set(source_df.columns)
    t_cols = set(target_df.columns)
    missing = s_cols - t_cols
    assert not missing, f"Missing columns: {missing}"

    # Checksums on numeric columns
    for col_name in source_df.columns:
        if source_df.schema[col_name].dataType in ('LongType', 'IntegerType', 'DoubleType'):
            s_sum = source_df.agg({col_name: 'sum'}).collect()[0][0]
            t_sum = target_df.agg({col_name: 'sum'}).collect()[0][0]
            assert s_sum == t_sum, f"Checksum mismatch on {col_name}: {s_sum} vs {t_sum}"

    print("Migration validation PASSED")
```

## Step 5: Cutover Plan

For producers with active pipelines, generate a cutover plan:

1. **Pre-cutover**: Run migration, validate, keep old table as backup
2. **Cutover window**: Update pipeline code to write to Iceberg table
3. **Dual-write period** (optional): Write to both old and new for N hours
4. **Post-cutover**: Verify new pipeline, drop old table backup after 7 days
5. **Rollback plan**: If issues, revert pipeline to old table (still intact)
