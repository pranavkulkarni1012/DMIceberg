---
description: "Create, alter, or drop Iceberg tables with schema evolution. Use when a producer needs to define a new Iceberg table, modify an existing table schema, change partitioning, or set table properties."
---

# Iceberg Table DDL Management

## Goal
Generate production-ready code to create, alter, or drop Iceberg tables registered in AWS Glue Data Catalog. Output code for the producer's specific tech stack.

## Step 1: Gather Requirements

Ask the producer (if not already provided):

1. **Operation**: CREATE, ALTER, or DROP?
2. **Tech stack**: Glue PySpark / EMR PySpark / ECS Python / Lambda Python / ECS Java / Lambda Java?
3. **Table details**:
   - Database name and table name
   - S3 warehouse path (e.g., `s3://producer-bucket/warehouse/`)
   - AWS region
4. **For CREATE**:
   - Column definitions (name, type, nullable, comment)
   - Partition strategy (identity, bucket, truncate, year/month/day/hour transforms)
   - Sort order (if needed)
   - Table properties (format-version, write mode, etc.)
5. **For ALTER**:
   - What change? (add column, drop column, rename column, change type, change partitioning, update properties)
   - Details of the change
6. **For DROP**:
   - Purge data files? (default: yes)

## Step 2: Generate Code

### Iceberg Type Mapping Reference
| Logical Type | Spark SQL | PyIceberg | Java API |
|---|---|---|---|
| boolean | BOOLEAN | BooleanType() | Types.BooleanType.get() |
| int | INT | IntegerType() | Types.IntegerType.get() |
| long | BIGINT | LongType() | Types.LongType.get() |
| float | FLOAT | FloatType() | Types.FloatType.get() |
| double | DOUBLE | DoubleType() | Types.DoubleType.get() |
| decimal(p,s) | DECIMAL(p,s) | DecimalType(p,s) | Types.DecimalType.of(p,s) |
| string | STRING | StringType() | Types.StringType.get() |
| date | DATE | DateType() | Types.DateType.get() |
| timestamp | TIMESTAMP | TimestampType() | Types.TimestampType.withoutZone() |
| timestamptz | TIMESTAMP_LTZ | TimestamptzType() | Types.TimestampType.withZone() |
| binary | BINARY | BinaryType() | Types.BinaryType.get() |
| uuid | STRING | UUIDType() | Types.UUIDType.get() |
| list | ARRAY<T> | ListType(element_id, T) | Types.ListType.ofRequired(id, T) |
| map | MAP<K,V> | MapType(key_id, K, val_id, V) | Types.MapType.ofRequired(kid, vid, K, V) |
| struct | STRUCT<...> | StructType(fields...) | Types.StructType.of(fields...) |

### Partition Transform Reference
| Transform | Spark SQL | PyIceberg | Java API |
|---|---|---|---|
| Identity | `PARTITIONED BY (col)` | `PartitionField(source_id, field_id, IdentityTransform())` | `PartitionSpec.builderFor(schema).identity(col)` |
| Year | `PARTITIONED BY (years(col))` | `PartitionField(source_id, field_id, YearTransform())` | `.year(col)` |
| Month | `PARTITIONED BY (months(col))` | `PartitionField(source_id, field_id, MonthTransform())` | `.month(col)` |
| Day | `PARTITIONED BY (days(col))` | `PartitionField(source_id, field_id, DayTransform())` | `.day(col)` |
| Hour | `PARTITIONED BY (hours(col))` | `PartitionField(source_id, field_id, HourTransform())` | `.hour(col)` |
| Bucket(N) | `PARTITIONED BY (bucket(N, col))` | `PartitionField(source_id, field_id, BucketTransform(N))` | `.bucket(col, N)` |
| Truncate(W) | `PARTITIONED BY (truncate(W, col))` | `PartitionField(source_id, field_id, TruncateTransform(W))` | `.truncate(col, W)` |

### For PySpark (Glue / EMR)

Generate a complete Spark SQL DDL statement. Always include:
- Catalog prefix: `glue_catalog.<database>.<table>`
- `USING iceberg`
- `TBLPROPERTIES` with at minimum: `'format-version'='2'`
- Spark session config for Iceberg + Glue Catalog (see CLAUDE.md)
- LOCATION pointing to S3

```python
# CREATE example structure
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.{database}.{table} (
        {column_definitions}
    )
    USING iceberg
    PARTITIONED BY ({partition_spec})
    LOCATION 's3://{bucket}/warehouse/{database}/{table}'
    TBLPROPERTIES (
        'format-version' = '2',
        'write.distribution-mode' = 'hash',
        'write.parquet.compression-codec' = 'zstd'
    )
""")

# ALTER examples
spark.sql("ALTER TABLE glue_catalog.db.tbl ADD COLUMNS (new_col STRING)")
spark.sql("ALTER TABLE glue_catalog.db.tbl ALTER COLUMN col TYPE BIGINT")
spark.sql("ALTER TABLE glue_catalog.db.tbl DROP COLUMN old_col")
spark.sql("ALTER TABLE glue_catalog.db.tbl RENAME COLUMN old_name TO new_name")
spark.sql("ALTER TABLE glue_catalog.db.tbl SET TBLPROPERTIES ('key'='value')")
spark.sql("ALTER TABLE glue_catalog.db.tbl ADD PARTITION FIELD bucket(16, col)")
spark.sql("ALTER TABLE glue_catalog.db.tbl DROP PARTITION FIELD col")

# DROP
spark.sql("DROP TABLE IF EXISTS glue_catalog.db.tbl PURGE")
```

For Glue jobs, include the `--datalake-formats iceberg` job parameter and `--conf` spark config.
For EMR, include the iceberg-spark-runtime JAR in spark-submit.

### For PyIceberg (ECS / Lambda Python)

Generate code using PyIceberg's catalog API:

```python
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType, ...
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, BucketTransform, ...

catalog = GlueCatalog("glue_catalog", **{
    "warehouse": "s3://{bucket}/warehouse/",
    "glue.region": "{region}",
    "s3.region":   "{region}",
})

# CREATE
schema = Schema(
    NestedField(field_id=1, name="col1", field_type=StringType(), required=True),
    NestedField(field_id=2, name="col2", field_type=LongType(), required=False),
    # ...
)
partition_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=BucketTransform(num_buckets=16), name="col2_bucket")
)
table = catalog.create_table(
    identifier="{database}.{table}",
    schema=schema,
    partition_spec=partition_spec,
    properties={"format-version": "2"}
)

# ALTER - schema evolution
with table.update_schema() as update:
    update.add_column("new_col", StringType())
    # update.rename_column("old", "new")
    # update.delete_column("col")
    # update.update_column("col", LongType())

# ALTER - partition evolution
with table.update_spec() as update:
    update.add_field("col", BucketTransform(16), "col_bucket")

# DROP
catalog.drop_table("{database}.{table}")
```

Include `pyiceberg[glue,s3fs,pyarrow]>=0.7.0` in requirements.

### For Java API (ECS / Lambda Java)

Generate code using Iceberg Java API:

```java
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

// Catalog setup. glue.region + s3.region are REQUIRED to prevent
// ambient AWS SDK region resolution (env/IMDS/profile) from triggering
// cross-region S3 access, which is disallowed on this platform.
GlueCatalog catalog = new GlueCatalog();
catalog.initialize("glue_catalog", ImmutableMap.of(
    "warehouse",   "s3://{bucket}/warehouse/",
    "io-impl",     "org.apache.iceberg.aws.s3.S3FileIO",
    "glue.region", "{region}",
    "s3.region",   "{region}"
));

// CREATE
Schema schema = new Schema(
    Types.NestedField.required(1, "col1", Types.StringType.get()),
    Types.NestedField.optional(2, "col2", Types.LongType.get())
);
PartitionSpec spec = PartitionSpec.builderFor(schema)
    .bucket("col2", 16)
    .build();
TableIdentifier tableId = TableIdentifier.of("{database}", "{table}");
Table table = catalog.createTable(tableId, schema, spec,
    "s3://{bucket}/warehouse/{database}/{table}",
    ImmutableMap.of("format-version", "2"));

// ALTER
table.updateSchema()
    .addColumn("new_col", Types.StringType.get())
    .commit();

table.updateSpec()
    .addField(Expressions.bucket("col", 16))
    .commit();

// DROP
catalog.dropTable(tableId, true); // true = purge
```

Include Maven/Gradle dependencies:
```xml
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
```

## Step 3: Validate Output

Before presenting the code:
1. Verify all column types map correctly to the chosen API
2. Verify partition transforms are valid for the column types (e.g., day/month/year only on date/timestamp)
3. Verify table properties are valid Iceberg properties
4. Verify S3 paths follow the warehouse convention
5. Include necessary imports and dependency declarations
6. Add inline comments explaining non-obvious choices
