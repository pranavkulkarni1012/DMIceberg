# PyIceberg Producer Recipe

Copy-pasteable flow for a Python producer on ECS or Lambda using PyIceberg.

**Read [`docs/using-dmiceberg.md`](../using-dmiceberg.md) first.** This recipe assumes you understand the 8-phase lifecycle and skill reference.

---

## Hypothetical producer: `datamesh-producer-kore`

| Setting | Value |
|---------|-------|
| Producer name | `datamesh-producer-kore` |
| Stack | ECS Fargate (container), Python 3.12, PyIceberg 0.8 |
| Source | REST API polled every 5 minutes |
| Target | `s3://kore-confirmed-us-east-1/warehouse/` (database `kore_confirmed`, table `user_events`) |
| Primary region | `us-east-1` |
| DR region | `us-west-2` |
| DR bucket | `s3://kore-confirmed-us-west-2/warehouse/` |
| Orchestrator | EventBridge → ECS RunTask |
| Format version | 2 |

Same recipe applies to Lambda Python — differences called out under "Lambda-specific notes" at the end.

---

## Phase 0: Profile setup

Run `/iceberg-profile-setup`. Pick `aws-glue-datamesh-strict` preset if you need the no-cross-region-S3 / no-MRAP constraints. Pick `aws-glue-central-lake` if your org allows cross-region S3.

## Phase 1: Assess (`/iceberg-onboard`)

Output will typically recommend:

- PyIceberg 0.8+ (canonical `glue.region` / `s3.region` keys landed in 0.7; earlier `region_name` is non-canonical).
- Batch size tuning — PyIceberg writes are single-node, so very large batches OOM. Break into 100k-1M row chunks.
- Consider whether you need `MERGE` semantics. PyIceberg does not have a native MERGE; you'll implement it as read-modify-write if required.

## Phase 2: Create (`/iceberg-ddl`)

```python
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

catalog = GlueCatalog("glue_catalog", **{
    "warehouse": "s3://kore-confirmed-us-east-1/warehouse/",
    "glue.region": "us-east-1",
    "s3.region":   "us-east-1",
})

schema = Schema(
    NestedField(1, "event_id",   StringType(),    required=True),
    NestedField(2, "event_time", TimestampType(), required=True),
    NestedField(3, "user_id",    StringType()),
    NestedField(4, "payload",    StringType()),
    NestedField(5, "region",     StringType(),    required=True),
)

partition_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=DayTransform(),      name="event_date"),
    PartitionField(source_id=5, field_id=1001, transform=IdentityTransform(), name="region"),
)

catalog.create_namespace_if_not_exists("kore_confirmed")
table = catalog.create_table(
    identifier="kore_confirmed.user_events",
    schema=schema,
    partition_spec=partition_spec,
    properties={
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
        "write.target-file-size-bytes": "134217728",
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "10",
    },
)
```

**Both `glue.region` and `s3.region` are required.** `glue.region` picks the Glue Catalog endpoint; `s3.region` picks the S3FileIO endpoint. Omitting either falls back to boto3's default region discovery, which is a frequent source of subtle cross-region bugs.

## Phase 3: Backfill (`/iceberg-migrate`)

### Option A: `add_files` (fastest, no rewrite)

```python
from pyiceberg.catalog.glue import GlueCatalog

catalog = GlueCatalog(...)
table = catalog.load_table("kore_confirmed.user_events")

file_paths = [
    "s3://kore-confirmed-us-east-1/legacy/events/date=2026-04-01/region=us-east-1/part-00000.parquet",
    "s3://kore-confirmed-us-east-1/legacy/events/date=2026-04-01/region=us-east-1/part-00001.parquet",
    # ... enumerate via boto3 list_objects_v2
]

table.add_files(
    file_paths=file_paths,
    snapshot_properties={"source": "legacy-parquet-backfill"},
    check_duplicate_files=True,
)
```

Set `check_duplicate_files=False` only if you've independently verified the files aren't already in the table — `add_files` raises `ValueError` on duplicates by default.

### Option B: Read + write via PyArrow

For cases where the source schema needs transformation:

```python
import pyarrow as pa
import pyarrow.parquet as pq

arrow_table = pq.read_table("s3://kore-confirmed-us-east-1/legacy/events/")

# ns → us downcast (Iceberg v2 does not accept ns timestamps)
if pa.types.is_timestamp(arrow_table.schema.field("event_time").type) \
   and arrow_table.schema.field("event_time").type.unit == "ns":
    arrow_table = arrow_table.set_column(
        arrow_table.schema.get_field_index("event_time"),
        "event_time",
        arrow_table["event_time"].cast(pa.timestamp("us")),
    )

table.append(arrow_table)
```

### Name-mapping property

If source Parquet has no embedded field IDs:

```python
table = catalog.load_table("kore_confirmed.user_events")
with table.transaction() as tx:
    tx.set_properties({
        "schema.name-mapping.default": '[{"field-id": 1, "names": ["event_id"]}, ...]'
    })
```

## Phase 4: Dual-write

In your existing ECS task:

```python
# Existing Parquet write — KEEP
write_parquet_batch(confirmed_records, "s3://kore-confirmed-us-east-1/confirmed/events/")

# NEW Iceberg write — ADD
arrow_table = pa.Table.from_pylist(
    confirmed_records,
    schema=iceberg_pyarrow_schema,
)
table.append(arrow_table)
```

Wrap both in a single try/except so a failure in either rolls back the whole batch.

```python
try:
    write_parquet_batch(confirmed_records, parquet_path)
    table.append(arrow_table)
    commit_batch_checkpoint(batch_id)
except Exception:
    # Leave the checkpoint unadvanced; next run will retry
    raise
```

## Phase 5: Reconciliation + cutover

```python
from datetime import date, timedelta
import pyarrow.parquet as pq

yesterday = date.today() - timedelta(days=1)

parquet_arrow = pq.read_table(
    f"s3://kore-confirmed-us-east-1/confirmed/events/event_date={yesterday}/",
)
iceberg_arrow = table.scan(
    row_filter=f"event_date = '{yesterday}'"
).to_arrow()

assert parquet_arrow.num_rows == iceberg_arrow.num_rows, \
    f"Mismatch: parquet={parquet_arrow.num_rows}, iceberg={iceberg_arrow.num_rows}"
```

Run on a cron inside ECS for the reconciliation window. Cutover = remove the Parquet write line from the producer code.

## Phase 6: Maintain (`/iceberg-maintenance`)

PyIceberg does not have native maintenance procedures in 0.8 — run maintenance via a scheduled Spark job (PySpark on Glue or EMR) against the same table, OR use the pyiceberg CLI for snapshot expiration:

```python
# Snapshot expiration — retain 7 days.
# PyIceberg's expire API takes an *epoch-millis* timestamp, not a datetime.
from datetime import datetime, timedelta, timezone

cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)

table.expire_snapshots() \
    .expire_older_than(cutoff_ms) \
    .commit()
```

The PyIceberg API is `table.expire_snapshots().expire_older_than(<epoch-ms>).commit()` — not a `tx.expire_snapshots_older_than(...)` transaction method. (Exists only on the `Table` object, pre-0.8.)

**Compaction in 0.8 is limited.** Schedule a small Glue job that calls `system.rewrite_data_files` once a day on this table — it's the pragmatic answer until PyIceberg adds first-class compaction.

**Orphan cleanup** — PyIceberg has no built-in. Either run a Spark job (`system.remove_orphan_files`) or implement a custom scanner that lists S3, compares to manifest references, and deletes with a 14-day grace window.

## Phase 7: Multi-region (`/iceberg-multi-region`)

Generated repointing utility in Python (runs on Lambda in us-west-2):

```python
import json
import fastavro
import boto3
from io import BytesIO

s3 = boto3.client("s3")
SRC_BUCKET = "kore-confirmed-us-east-1"
DST_BUCKET = "kore-confirmed-us-west-2"

def repoint_path(p: str) -> str:
    return p.replace(f"s3://{SRC_BUCKET}/", f"s3://{DST_BUCKET}/")

def rewrite_metadata_json(key: str):
    obj = s3.get_object(Bucket=DST_BUCKET, Key=key)
    meta = json.loads(obj["Body"].read())
    meta["location"] = repoint_path(meta["location"])
    for snap in meta.get("snapshots", []):
        snap["manifest-list"] = repoint_path(snap["manifest-list"])
    s3.put_object(
        Bucket=DST_BUCKET, Key=key,
        Body=json.dumps(meta).encode(),
    )

def rewrite_avro(key: str, path_fields: list[str]):
    src = s3.get_object(Bucket=DST_BUCKET, Key=key)["Body"].read()
    reader = fastavro.reader(BytesIO(src))
    # Preserve file-level metadata (iceberg.schema, content, etc.)
    extra_metadata = {
        k: v for k, v in reader.metadata.items()
        if k not in ("avro.schema", "avro.codec")
    }
    records = []
    for rec in reader:
        for field in path_fields:
            if field in rec and rec[field]:
                rec[field] = repoint_path(rec[field])
        records.append(rec)
    buf = BytesIO()
    fastavro.writer(
        buf, reader.writer_schema, records,
        codec=reader.metadata.get("avro.codec", "null"),
        metadata=extra_metadata,
    )
    s3.put_object(Bucket=DST_BUCKET, Key=key, Body=buf.getvalue())
```

Full skill output includes Glue registration, drop-and-register rollback, CRR sync schedule, and monitoring.

## Phase 8: Failover flip

Same runbook as the PySpark recipe. The only difference: after flipping, deploy your ECS task definition with env vars pointing to the new region:

```
WAREHOUSE_BUCKET=kore-confirmed-us-west-2
GLUE_REGION=us-west-2
S3_REGION=us-west-2
```

ECS writes natively in us-west-2 — no repointing needed on DR-native writes.

---

## Lambda-specific notes

If you're running the same producer as a Lambda instead of ECS:

### Packaging

- PyIceberg + pyarrow brings ~80MB of deps — use a Lambda layer for them or a container image.
- Set Lambda memory to **at least 1024 MB.** PyIceberg holds the entire written batch in memory. 2048 MB is safer for batches > 100k rows.

### Cold start

- First invocation can take 5-10 seconds to load PyIceberg + fetch the current `metadata.json`. Use provisioned concurrency for latency-sensitive producers.

### Timeout

- Lambda has a 15-minute max. Size batches accordingly. If a single invocation approaches the limit, split the batch across multiple invocations with idempotent checkpointing.

### IAM

Minimal Lambda execution role for Iceberg writes:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
    "glue:GetTable", "glue:UpdateTable", "glue:GetDatabase",
    "glue:GetTableVersions", "glue:CreateTable"
  ],
  "Resource": "*"
}
```

Scope `Resource` down to your specific buckets and Glue database ARNs in production.

### Cold-path vs hot-path writes

Lambda Iceberg writes create a new snapshot per invocation, which can accumulate to thousands of snapshots per day on a hot table. Mitigate with:

- Batching at the Lambda level (buffer N records or T seconds in memory before writing)
- More aggressive snapshot expiration (daily rather than weekly)
- SQS-batched invocation (up to 10 records per Lambda call) rather than per-record

---

## Gotchas specific to PyIceberg

- **`region_name` does not work.** Use `glue.region` and `s3.region`. Old examples on the internet using `region_name` are from pre-0.7.
- **`pa.timestamp('ns')` rejected.** Iceberg v2 requires us. Downcast explicitly.
- **No MERGE.** Read-modify-write pattern: `table.scan(row_filter=...).to_arrow()`, merge with source in PyArrow, `table.overwrite(result, overwrite_filter=...)`.
- **`add_files` raises on duplicates.** Pass `check_duplicate_files=False` only when you've verified.
- **Writes are single-node.** Not distributed. Tune batch size to available memory.
- **No built-in compaction.** Run a scheduled Spark job for compaction + orphan cleanup; PyIceberg handles write + snapshot expiration only.
