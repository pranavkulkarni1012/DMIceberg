---
description: "Generate code to insert, upsert (MERGE), delete, or overwrite data in Iceberg tables. Use when a producer needs to ingest data, perform upserts, delete records, or overwrite partitions in their Iceberg table."
user_invocable: true
---

# Iceberg Data Operations

## Goal
Generate production-ready data ingestion and manipulation code for Iceberg tables. Output code for the producer's specific tech stack.

## Step 1: Gather Requirements

Ask the producer (if not already provided):

1. **Operation type**: INSERT / UPSERT (MERGE) / DELETE / OVERWRITE PARTITION?
2. **Tech stack**: Glue PySpark / EMR PySpark / ECS Python / Lambda Python / ECS Java / Lambda Java?
3. **Source data**:
   - Source format (Parquet, CSV, JSON, JDBC, Kafka, Kinesis, API)?
   - Source location (S3 path, JDBC URL, stream ARN)?
   - Schema of source data
4. **Target Iceberg table**: database.table name, S3 warehouse path, region
5. **For UPSERT**:
   - Merge key columns (columns to match on)
   - When matched: UPDATE SET all / specific columns?
   - When not matched: INSERT?
   - When matched and condition: DELETE?
6. **For DELETE**: Filter condition
7. **For OVERWRITE**: Dynamic or static partition overwrite?
8. **Write mode preferences**:
   - merge-on-read vs copy-on-write (default: merge-on-read for upsert-heavy, copy-on-write for read-heavy)
   - Target file size (default: 128MB for Parquet)
9. **Batch or streaming**?

## Step 2: Generate Code

### For PySpark (Glue / EMR)

Always include Spark session config for Iceberg + Glue Catalog.

**INSERT (Append)**:
```python
# From DataFrame
df.writeTo("glue_catalog.{database}.{table}") \
  .option("fan-out-enabled", "true") \
  .append()

# From SQL
spark.sql("""
    INSERT INTO glue_catalog.{database}.{table}
    SELECT * FROM source_view
""")
```

**UPSERT (MERGE INTO)**:
```python
# Register source as temp view
source_df.createOrReplaceTempView("source_data")

spark.sql("""
    MERGE INTO glue_catalog.{database}.{table} t
    USING source_data s
    ON t.{merge_key} = s.{merge_key}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

For conditional merge:
```python
spark.sql("""
    MERGE INTO glue_catalog.{database}.{table} t
    USING source_data s
    ON t.id = s.id
    WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
    WHEN MATCHED AND s.op = 'UPDATE' THEN UPDATE SET
        t.col1 = s.col1,
        t.col2 = s.col2,
        t.updated_at = s.updated_at
    WHEN NOT MATCHED THEN INSERT *
""")
```

**DELETE**:
```python
spark.sql("""
    DELETE FROM glue_catalog.{database}.{table}
    WHERE {condition}
""")
```

**OVERWRITE PARTITION**:
```python
# Dynamic overwrite (overwrites only partitions present in source)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.writeTo("glue_catalog.{database}.{table}").overwritePartitions()

# Static overwrite
spark.sql("""
    INSERT OVERWRITE glue_catalog.{database}.{table}
    PARTITION (date = '2024-01-15')
    SELECT col1, col2 FROM source_data
""")
```

**Streaming ingestion (Spark Structured Streaming)**:
```python
# From Kafka/Kinesis
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "{servers}") \
    .option("subscribe", "{topic}") \
    .load()

parsed_df = stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", "glue_catalog.{database}.{table}") \
    .option("checkpointLocation", "s3://{bucket}/checkpoints/{table}") \
    .option("fanout-enabled", "true") \
    .trigger(processingTime="5 minutes") \
    .start()
```

For Glue jobs, always include:
- `--datalake-formats iceberg` in job parameters
- `--enable-glue-datacatalog true`

### For PyIceberg (ECS / Lambda Python)

```python
from pyiceberg.catalog.glue import GlueCatalog
import pyarrow as pa

catalog = GlueCatalog("glue_catalog", **{
    "warehouse": "s3://{bucket}/warehouse/",
    "region_name": "{region}"
})
table = catalog.load_table("{database}.{table}")

# INSERT - from PyArrow table
arrow_table = pa.table({
    "col1": ["a", "b", "c"],
    "col2": [1, 2, 3],
    "event_time": [datetime(2024,1,1), datetime(2024,1,2), datetime(2024,1,3)]
})
table.append(arrow_table)

# INSERT - from Parquet file on S3
import pyarrow.parquet as pq
arrow_table = pq.read_table("s3://{source_bucket}/{path}")
table.append(arrow_table)

# OVERWRITE (full table or filtered)
table.overwrite(arrow_table)
table.overwrite(arrow_table, overwrite_filter=EqualTo("date", "2024-01-15"))

# DELETE
from pyiceberg.expressions import EqualTo, GreaterThan, And
table.delete(delete_filter=EqualTo("status", "expired"))
table.delete(delete_filter=And(
    EqualTo("region", "us-east-1"),
    GreaterThan("age_days", 90)
))

# UPSERT (merge) - PyIceberg supports overwrite with filter for upsert patterns
# For true MERGE, read-modify-write pattern:
scan = table.scan(row_filter=EqualTo("id", target_id))
existing = scan.to_arrow()
# ... merge logic with PyArrow ...
table.overwrite(merged_table, overwrite_filter=EqualTo("partition_col", partition_value))
```

Include `pyiceberg[glue,s3fs,pyarrow]` in requirements.

### For Java API (ECS / Lambda Java)

```java
import org.apache.iceberg.*;
import org.apache.iceberg.data.*;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;

Table table = catalog.loadTable(TableIdentifier.of("{database}", "{table}"));

// INSERT - write data files
Schema schema = table.schema();
GenericRecord record = GenericRecord.create(schema);

OutputFile outputFile = table.io().newOutputFile(
    table.locationProvider().newDataLocation("data-001.parquet")
);
DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
    .schema(schema)
    .createWriterFunc(GenericParquetWriter::buildWriter)
    .overwrite()
    .build();

try {
    for (Record r : records) {
        writer.write(r);
    }
} finally {
    writer.close();
}

// Commit the data file
DataFile dataFile = writer.toDataFile();
table.newAppend()
    .appendFile(dataFile)
    .commit();

// DELETE - positional or equality
table.newDelete()
    .deleteFromRowFilter(Expressions.equal("status", "expired"))
    .commit();

// OVERWRITE partition
table.newOverwrite()
    .overwriteByRowFilter(Expressions.equal("date", "2024-01-15"))
    .addFile(newDataFile)
    .commit();

// ROW-LEVEL UPSERT (using RowDelta for merge-on-read)
table.newRowDelta()
    .addRows(insertFile)
    .addDeletes(deleteFile)
    .commit();
```

Maven dependencies:
```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-core</artifactId>
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
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-aws</artifactId>
    <version>1.7.1</version>
</dependency>
```

## Step 3: Additional Patterns

### Write Performance Tuning
Include these table properties for write optimization:
```
'write.distribution-mode' = 'hash'          -- for partitioned tables
'write.parquet.compression-codec' = 'zstd'  -- best compression ratio
'write.target-file-size-bytes' = '134217728' -- 128MB target file size
'write.metadata.delete-after-commit.enabled' = 'true'
'write.metadata.previous-versions-max' = '10'
```

### Merge-on-Read vs Copy-on-Write
- **Copy-on-Write** (`'write.delete.mode'='copy-on-write'`): Rewrites data files on delete/update. Better for read-heavy workloads.
- **Merge-on-Read** (`'write.delete.mode'='merge-on-read'`): Writes delete files. Better for write-heavy/upsert workloads. Requires periodic compaction.

### Error Handling
Always wrap write operations with:
- Retry logic for S3 throttling (exponential backoff)
- Commit conflict handling (OptimisticLockException in Java, CommitFailedException in Spark)
- Data validation before write (null checks on required fields, type compatibility)

## Step 4: Validate Output

1. Verify source schema is compatible with target Iceberg schema
2. Verify merge keys exist in both source and target
3. Verify partition filters are valid for overwrite operations
4. Include all necessary imports and dependencies
5. Include error handling for commit conflicts
