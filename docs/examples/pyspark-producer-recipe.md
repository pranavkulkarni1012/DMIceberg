# PySpark Producer Recipe

Copy-pasteable flow for a PySpark producer (Glue or EMR) migrating Parquet → Iceberg with multi-region resilience.

**Read [`docs/using-iceshelf.md`](../using-iceshelf.md) first.** This recipe assumes you understand the 8-phase lifecycle, the platform constraints, and the skill reference. Everything below is stack-specific glue.

---

## Hypothetical producer: `datamesh-producer-brok`

| Setting | Value |
|---------|-------|
| Producer name | `datamesh-producer-brok` |
| Stack | AWS Glue 5.1 (Spark 3.5.6, Iceberg 1.10.0), PySpark |
| Source | Staging Parquet in `s3://brok-staging-us-east-1/raw/` |
| Existing target | Confirmed Parquet in `s3://brok-confirmed-us-east-1/confirmed/events/` partitioned by `event_date`, `region` |
| Iceberg target | `s3://brok-confirmed-us-east-1/warehouse/` (database `brok_confirmed`, table `events`) |
| Primary region | `us-east-1` |
| DR region | `us-west-2` |
| DR bucket | `s3://brok-confirmed-us-west-2/warehouse/` |
| Orchestrator | Step Functions |
| Format version | 2 |

Substitute your own bucket/database/table/region names in everything below.

---

## Phase 0: Profile setup

Run `/iceberg-profile-setup`. It will write `profiles/datamesh-producer-brok.yaml` containing the above settings. Every subsequent skill reads from this file.

## Phase 1: Assess (`/iceberg-onboard`)

Run `/iceberg-onboard` with your profile. The orchestrator delegates to `iceberg-architect` which inspects your Glue job scripts, the S3 layout, and produces a migration strategy document. Review it before proceeding — particularly the partition spec recommendation.

**PySpark-specific things to watch for in the output:**

- Timestamp precision in your source Parquet. If it's nanosecond (Spark default for some writers), you'll need the ns→us downcast before phase 3.
- Existing Parquet files that lack embedded field IDs. The migrate skill will add a `schema.name-mapping.default` property to handle this.
- Partition column cardinality. If `region` has ~10 values and `event_date` is daily, you're fine. If you have a high-cardinality column in your partition spec, the architect will flag it.

## Phase 2: Create (`/iceberg-ddl`)

Run `/iceberg-ddl` with the schema from Phase 1. For brok, the generated SQL looks like:

```python
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://brok-confirmed-us-east-1/warehouse/")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

spark.sql("""
    CREATE TABLE glue_catalog.brok_confirmed.events (
        event_id STRING,
        event_time TIMESTAMP,
        event_date DATE,
        region STRING,
        user_id STRING,
        payload STRING
    )
    USING iceberg
    PARTITIONED BY (event_date, region)
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes' = '134217728',
        'write.distribution-mode' = 'hash',
        'write.delete.mode' = 'merge-on-read',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '10'
    )
""")
```

**Glue job parameters** for any Glue job that touches this table:

```
--datalake-formats iceberg
--enable-glue-datacatalog true
```

## Phase 3: Backfill (`/iceberg-migrate`)

Three options; `iceberg-architect` picks one in Phase 1:

### Option A: `system.add_files` (fastest, file-level registration)

Best when the source Parquet schema already matches your Iceberg schema and field IDs aren't needed.

```python
spark.sql("""
    CALL glue_catalog.system.add_files(
        table => 'brok_confirmed.events',
        source_table => '`parquet`.`s3://brok-confirmed-us-east-1/confirmed/events/`',
        check_duplicate_files => true
    )
""")
```

Add the name-mapping first so readers can find fields by name:

```python
spark.sql("""
    ALTER TABLE glue_catalog.brok_confirmed.events
    SET TBLPROPERTIES (
        'schema.name-mapping.default' = '[{"field-id": 1, "names": ["event_id"]}, {"field-id": 2, "names": ["event_time"]}, ...]'
    )
""")
```

### Option B: CTAS (full rewrite, safest)

```python
spark.sql("""
    CREATE TABLE glue_catalog.brok_confirmed.events
    USING iceberg
    PARTITIONED BY (event_date, region)
    TBLPROPERTIES ('format-version' = '2')
    AS SELECT * FROM parquet.`s3://brok-confirmed-us-east-1/confirmed/events/`
""")
```

### Option C: `system.snapshot` (non-destructive shadow)

```python
spark.sql("""
    CALL glue_catalog.system.snapshot(
        source_table => 'brok_confirmed.events_parquet',
        table => 'brok_confirmed.events'
    )
""")
```

### ns → us timestamp downcast

If your source Parquet has nanosecond timestamps, Iceberg v2 requires microsecond. Downcast before `add_files`:

```python
from pyspark.sql.functions import col, expr

df = spark.read.parquet("s3://brok-confirmed-us-east-1/confirmed/events/")
df = df.withColumn("event_time", expr("cast(event_time as timestamp)"))  # drops to us precision
df.writeTo("glue_catalog.brok_confirmed.events").append()
```

## Phase 4: Dual-write

Add the Iceberg write **alongside** the existing Parquet write. Do not remove Parquet yet.

```python
# Existing block — KEEP
confirmed_df.write.mode("append") \
    .partitionBy("event_date", "region") \
    .parquet("s3://brok-confirmed-us-east-1/confirmed/events/")

# NEW block — ADD
confirmed_df.writeTo("glue_catalog.brok_confirmed.events") \
    .option("fanout-enabled", "true") \
    .append()

# Glue bookmark commit goes AFTER both writes succeed
job.commit()
```

**Critical:** `job.commit()` goes on the success path only, not in a `finally` block. If it fires on exception, Glue advances the bookmark past failed records and they're lost forever.

## Phase 5: Reconciliation window + cutover (manual)

Run dual-write for 2-4 weeks. During that window:

**Daily reconciliation check** (run via Step Functions or a Glue job):

```python
parquet_df = spark.read.parquet("s3://brok-confirmed-us-east-1/confirmed/events/") \
    .filter("event_date = current_date() - 1")
iceberg_df = spark.table("glue_catalog.brok_confirmed.events") \
    .filter("event_date = current_date() - 1")

parquet_count = parquet_df.count()
iceberg_count = iceberg_df.count()

assert parquet_count == iceberg_count, f"Mismatch: parquet={parquet_count}, iceberg={iceberg_count}"

# Spot-check a sample of rows
sample = parquet_df.sample(0.001).join(
    iceberg_df, on=["event_id"], how="left_anti"
).count()
assert sample == 0, f"{sample} rows in Parquet missing from Iceberg"
```

Alert on any mismatch. Only cut over when:

- Counts match for the full reconciliation window
- Downstream consumers have validated Iceberg reads against Parquet
- You have a documented rollback (restore the Parquet write block)

**Cutover day:** remove the Parquet write block from the Glue job. Iceberg is now the sole target. Leave the Parquet data in place for the rollback window (another 2-4 weeks), then delete.

## Phase 6: Maintain (`/iceberg-maintenance`)

Run `/iceberg-maintenance`. It generates scheduled jobs for your orchestrator. For brok on Step Functions:

### Daily compaction (midnight UTC)

```python
spark.sql("""
    CALL glue_catalog.system.rewrite_data_files(
        table => 'brok_confirmed.events',
        options => map(
            'target-file-size-bytes', '134217728',
            'min-input-files', '5'
        )
    )
""")

spark.sql("""
    CALL glue_catalog.system.rewrite_position_delete_files(
        table => 'brok_confirmed.events'
    )
""")
```

### Weekly snapshot expiration (Sunday 02:00 UTC)

Retain 7 days of snapshots:

```python
spark.sql("""
    CALL glue_catalog.system.expire_snapshots(
        table => 'brok_confirmed.events',
        older_than => TIMESTAMP '2026-04-12 00:00:00',
        retain_last => 10
    )
""")
```

Use a parameterized timestamp in the actual job — compute `current_timestamp() - INTERVAL 7 DAYS`.

### Monthly orphan cleanup (1st of month 03:00 UTC)

```python
spark.sql("""
    CALL glue_catalog.system.remove_orphan_files(
        table => 'brok_confirmed.events',
        older_than => TIMESTAMP '2026-03-19 00:00:00'
    )
""")
```

Older-than must be well past any in-flight commit (24h minimum, 14 days recommended) — otherwise you can delete data files that are in-flight but not yet committed.

## Phase 7: Multi-region (`/iceberg-multi-region`)

Run `/iceberg-multi-region`. It delegates to `iceberg-multi-region-planner` which generates:

### CRR configuration

S3 Cross-Region Replication from `s3://brok-confirmed-us-east-1/warehouse/` to `s3://brok-confirmed-us-west-2/warehouse/`. Replicate all objects; no filter. Delete markers replicated. Versioning enabled on both buckets.

### Repointing utility (Python, runs in us-west-2)

Rewrites every URI from `brok-confirmed-us-east-1` to `brok-confirmed-us-west-2` in:

1. `metadata.json`
2. `snap-*.avro` (manifest lists)
3. `*-m0.avro` (manifests)

Avro file-level metadata (`avro.codec`, `iceberg.schema`, `content`, `partition-spec-id`, etc.) is preserved via `fastavro.writer(..., metadata=extra_metadata)`.

### DR-region Glue Catalog registration

After repointing, register `brok_confirmed.events` in us-west-2 Glue, pointing to the repointed `metadata.json`. Use `register_table` (not `create`) — the metadata already exists.

### Sync schedule

EventBridge rule fires every 15 minutes → Step Functions → runs the repointing Lambda in us-west-2. On failure, alarm to PagerDuty and retry with exponential backoff (max 3 attempts).

## Phase 8: Failover flip runbook

### Flip primary → DR (us-east-1 is down)

1. **Stop brok writer in us-east-1.** Disable the EventBridge schedule that triggers the Glue ingestion job.
2. **Drain in-flight writes.** Wait for any running job to finish. Check CloudWatch for active Glue executions.
3. **Confirm CRR is caught up.** Check `ReplicationLatency` metric in us-east-1 bucket; wait for it to be zero. If us-east-1 is fully down, skip this and accept the last-known-good state in DR.
4. **Run repointing one more time in us-west-2.** Ensure DR has the latest metadata. Use the on-demand invocation of the repointing Lambda.
5. **Register latest metadata in us-west-2 Glue** (if the routine sync hasn't already). Use the drop-and-register pattern from the repointing skill, with the rollback-on-failure safeguard.
6. **Flip downstream readers.** Update consumer jobs' Spark config to use us-west-2 Glue Catalog. If consumers read via Athena, update Athena's region.
7. **Deploy brok writer in us-west-2** pointing to `s3://brok-confirmed-us-west-2/warehouse/`. Iceberg writes here are **native** — no CRR-then-repoint. DR is now the primary.
8. **Reverse CRR.** Enable CRR from `brok-confirmed-us-west-2` → `brok-confirmed-us-east-1` so we can fail back when us-east-1 recovers.

### Flip back: DR → primary (us-east-1 recovered)

Same flow, reversed:

1. Stop writer in us-west-2.
2. Drain writes; wait for reverse-CRR to catch up.
3. Run repointing in us-east-1 (rewriting paths from `-us-west-2` back to `-us-east-1`). The metadata in us-east-1 is stale from before the original failover — don't trust it; rebuild from replicated state.
4. Register repointed metadata in us-east-1 Glue.
5. Flip readers back to us-east-1.
6. Deploy writer back to us-east-1.
7. Re-enable forward CRR (us-east-1 → us-west-2); disable reverse.

### Post-flip verification

Run on both sides:

```python
spark.sql("SELECT COUNT(*), MAX(event_time) FROM glue_catalog.brok_confirmed.events").show()
```

Counts and max event time should align within the CRR latency window.

---

## Appendix: pre-flight checklist

Before Phase 4 (dual-write):

- [ ] Profile YAML committed to `profiles/`
- [ ] Iceberg table created + Glue Catalog entry verified
- [ ] Name-mapping property set if backfilling via `add_files`
- [ ] Timestamp precision downcast if source was ns
- [ ] Glue job parameters include `--datalake-formats iceberg`
- [ ] IAM role on Glue job has `glue:*` + `s3:*` on warehouse path

Before Phase 5 (cutover):

- [ ] 2+ weeks of reconciliation with zero mismatches
- [ ] Downstream consumers validated on Iceberg
- [ ] Rollback plan documented (restore Parquet write block)
- [ ] Old Parquet data retention agreed (2-4 weeks post-cutover)

Before Phase 7 (multi-region):

- [ ] DR-region bucket created in us-west-2
- [ ] CRR configured + smoke-tested (upload test object, verify replication)
- [ ] IAM role for repointing Lambda has read/write on both buckets + `glue:*` in DR region
- [ ] EventBridge schedule deployed
- [ ] Monitoring + alerting on repointing failures + CRR latency

Before Phase 8 (failover drill):

- [ ] Run a scheduled failover drill at least once per quarter in a non-prod environment
- [ ] Document runbook signed off by on-call team
- [ ] RTO/RPO targets agreed with stakeholders

---

## Gotchas specific to PySpark

- **`job.commit()` placement.** Success path only. A `finally` block advances the Glue bookmark on failure and you lose the failed batch forever.
- **`spark.sql.sources.partitionOverwriteMode = 'dynamic'`** must be set for `overwritePartitions()` to behave as expected.
- **Fanout-enabled on writes to many partitions.** `.option("fanout-enabled", "true")` avoids sorting by partition before write — saves a shuffle when you're writing to hundreds of partitions per job.
- **Streaming to Iceberg:** use `.toTable("glue_catalog.db.tbl")`, **not** `.option("path", "glue_catalog.db.tbl")`. The latter treats the value as a filesystem path and fails.
- **Glue 5.0 vs 5.1.** Glue 5.0 ships Iceberg 1.6.1 (updated to 1.7.1 in a later patch); Glue 5.1 ships Iceberg 1.10.0 with Spark 3.5.6. Some procedures (`rewrite_position_delete_files`) require 1.7+.
