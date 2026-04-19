# Migrating `datamesh-producer-brok` from Parquet to Iceberg

**Audience:** Engineers on a Data Mesh producer team whose pipeline reads
a staging area with Spark and writes Parquet files (partitioned by two or
more columns) to a conformed S3 location.

**Outcome:** Your conformed data lives in an Apache Iceberg table in the
AWS Glue Data Catalog, with ACID inserts/updates/deletes, scheduled
maintenance, and multi-region failover.

This guide drives you through the work by invoking skills and agents
already installed in this repo. You do not need to read the skill files
yourself -- each step says which slash command to run, what it will ask
you, and how to verify the result.

---

## Before you start: one-line model of what you have today

You own a Spark job (AWS Glue or EMR) that:

- Reads from a staging S3 path (Parquet or JSON / CSV).
- Applies transforms.
- Writes Parquet files to `s3://brok-conformed/<dataset>/` under a Hive-style
  directory layout like `country=US/event_date=2026-04-19/`.
- Produces one or more partitions keyed by **two or more columns** (for
  example `country` + `event_date`, or `product_line` + `region`).
- Is registered in the AWS Glue Data Catalog as a Parquet (or Hive) table.

You want the same pipeline to land data in an Iceberg table at
`s3://brok-conformed/warehouse/<database>/<dataset>/` with the same
partition semantics, snapshot-level ACID, and a DR region.

---

## The migration at a glance

| Phase | What you do | Skill / agent | Typical elapsed time |
|------:|-------------|---------------|:--------------------:|
| 0 | Prerequisites check | manual | 15 min |
| 1 | Assess and produce a plan | `/iceberg-onboard` | 20 min |
| 2 | Create the Iceberg target table | `/iceberg-ddl` | 15 min |
| 3 | Backfill historical data | `/iceberg-migrate` | varies (mins – hours) |
| 4 | Switch the Spark job's write path | `/iceberg-pipeline` + `/iceberg-data` | 30 min |
| 5 | Dual-write validation and cutover | manual + `/iceberg-info` | 2–7 days |
| 6 | Schedule maintenance | `/iceberg-maintenance` | 15 min |
| 7 | Add the DR region | `/iceberg-multi-region` | 1–2 hours |
| 8 | Rehearse the failover flip | manual (runbook below) | 30 min |

Each phase is safe to pause between. If something goes wrong in phases
2–4, you have not yet touched your production Parquet pipeline -- the
rollback is "stop".

---

## Phase 0 — Prerequisites

Confirm the following once. None of these are Iceberg-specific, but every
skill downstream assumes they are true.

1. **AWS Glue version**. Prefer Glue 5.1 (bundles Iceberg 1.10.0 on Spark
   3.5.6). Glue 5.0 is fine (launched with Iceberg 1.6.1, subsequently
   updated to 1.7.1 — confirm the currently bundled version in the AWS
   Glue release notes). Glue 4.0 works but is on the older Spark 3.3
   runtime and we recommend upgrading before migration.
2. **Glue job parameters** already include `--enable-glue-datacatalog true`.
   You will add `--datalake-formats iceberg` in Phase 4.
3. **IAM role for the Glue job** has:
   - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`
     on the conformed bucket and prefix
   - `glue:GetTable`, `glue:GetTables`, `glue:UpdateTable`,
     `glue:CreateTable`, `glue:GetDatabase`, `glue:CreateDatabase`
4. **Conformed bucket has versioning enabled**. Required for S3 CRR in
   Phase 7. Enable it now if it is off -- it is not disruptive.
5. **You know your warehouse path**. Convention on this platform is
   `s3://<producer-bucket>/warehouse/`. For this producer:
   `s3://brok-conformed/warehouse/`.
6. **You have a non-prod database / dataset you can run the migration in
   first**. Every step in this guide should be exercised against non-prod
   before touching prod.

When all six are true, continue.

---

## Phase 1 — Assess and produce a plan

**Run:** `/iceberg-onboard`

Before you invoke the skill, open the Spark job's entry point yourself
and skim it. You are about to delegate the detailed reading to the
architect agent, but knowing roughly where the read / transform /
write sections live makes it easier to interpret the plan and spot
anything the agent may have overlooked.

`/iceberg-onboard` is the single entry point. It spawns the
`iceberg-orchestrator` agent, which invokes `iceberg-architect` to
read your code, infer your schema and partition layout, and return a
migration plan.

### What it will ask you

- Path to the producer repo on disk (e.g. `C:\repos\datamesh-producer-brok`
  or `/mnt/repos/datamesh-producer-brok`). Point it at the directory that
  contains your Glue/EMR scripts.
- Whether you want multi-region from day one, or plan to add it later
  (Phase 7). Either answer is fine.
- Names for the target database and table in Glue. Convention:
  `brok_conformed.<dataset>`.

### What you get back

A structured plan with:

- Detected tech stack (it should say "AWS Glue PySpark" or "EMR PySpark").
- Your current schema, partition columns, and write pattern (append vs.
  overwrite vs. upsert).
- A **recommended partition spec** for Iceberg. With two or more
  partition columns, the architect will usually recommend one of:
  - `PARTITIONED BY (col_a, days(col_timestamp))` -- keep one identity
    column, convert a date/timestamp column to a `days()` transform.
  - `PARTITIONED BY (col_a, bucket(16, col_b))` -- when both columns
    have very high cardinality and would produce too many small files.
  - `PARTITIONED BY (col_a, col_b)` -- keep identity partitioning when
    both columns have manageable cardinality (this is the path most
    producers take on the first migration).
- A **migration approach**: `add_files` (non-destructive, fastest),
  `CTAS` (clean slate), or `snapshot` (read-only mirror). For a producer
  that already has Parquet in a well-understood layout, `add_files` is
  almost always the recommendation.
- Format version (2 -- the default) and write mode (copy-on-write unless
  you have upserts).
- A **risk table** covering data, schema, downtime, downstream
  compatibility, and cost.
- A **rollback plan** -- keep this open in a tab for the rest of the
  migration.

### Before you move on

- [ ] The plan names a specific partition spec. Write it down.
- [ ] The plan chose a specific migration approach. Write it down.
- [ ] Any downstream consumer (Athena, Redshift Spectrum, Presto, ...)
      named in the plan supports Iceberg v2. If the plan flagged an
      incompatible consumer, pause and upgrade or isolate that consumer
      before continuing.

---

## Phase 2 — Create the Iceberg target table

**Run:** `/iceberg-ddl`

This skill emits the `CREATE TABLE` statement that matches the plan from
Phase 1.

### What it will ask you

- Tech stack (answer: AWS Glue PySpark, same as your pipeline).
- Database and table name (from Phase 1).
- The schema and the partition spec from Phase 1.
- Target file size (leave at default 128 MB unless you have a reason).

### What it produces

- A PySpark script that opens a configured Spark session and runs
  `CREATE TABLE glue_catalog.brok_conformed.<dataset> (...) USING iceberg
  PARTITIONED BY (...) TBLPROPERTIES (...)`.
- A short IAM snippet confirming the Glue + S3 permissions needed.
- A smoke-test stub that creates a throwaway table, writes one row,
  reads it back, drops it.

### Do this

1. Run the smoke-test stub **against your non-prod Glue environment
   first**. If it fails here, it will fail in prod -- fix before moving on.
2. Run the real `CREATE TABLE` script.
3. Verify in the Glue console: the table should appear with table type
   `ICEBERG` and a `metadata_location` parameter pointing to
   `s3://brok-conformed/warehouse/brok_conformed/<dataset>/metadata/<N>-<uuid>.metadata.json`.
4. Optional: run `/iceberg-info` and ask it to print the table schema,
   partition spec, and properties. This reads the Iceberg metadata
   directly and is the best way to confirm what you created matches the
   plan.

### Before you move on

- [ ] Table exists in Glue, type = ICEBERG.
- [ ] Schema matches your Parquet schema (modulo the ns→us timestamp
      downcast -- see troubleshooting below).
- [ ] Partition spec matches the plan from Phase 1.
- [ ] `format-version = 2` in table properties.

---

## Phase 3 — Backfill historical data

**Run:** `/iceberg-migrate`

This is the step that copies or references your existing Parquet files
into the new Iceberg table. The approach is whichever the Phase 1 plan
chose.

### Path A: `add_files` (recommended for most producers)

`add_files` registers existing Parquet files **in place** as data files
of the Iceberg table. It does **not** rewrite or move any bytes. Runtime
scales with the number of files, not their total size.

Safety properties:

- Non-destructive. Your Parquet files stay where they are; the Iceberg
  table simply references them.
- Safe to re-run with a narrower partition filter, but **not**
  silently idempotent across the whole table: by default, `add_files`
  raises if it encounters a file already registered in any Iceberg
  table. If a re-run is necessary, either run with
  `check_duplicate_files => false` (only when you are certain the
  overlap is intentional) or narrow the source filter to unregistered
  partitions.
- Reversible. Drop the Iceberg table with `DROP TABLE ...` (no `PURGE`
  keyword) and your Parquet files are still there -- only the catalog
  entry and Iceberg metadata are removed.

Caveats the skill will enforce:

- Existing Parquet files must not already be referenced by another
  Iceberg table.
- `schema.name-mapping.default` is registered on the Iceberg table so
  that Parquet files without Iceberg field IDs are still readable.
- Nanosecond-precision timestamps (`pa.timestamp('ns')`) are downcast to
  microseconds before registration; Iceberg format-v2 cannot store ns.

### Path B: `CTAS` (clean slate)

If the plan flagged schema irregularities (inconsistent column order,
mixed nullability, pre-existing partition pruning issues), the skill
will recommend `CREATE TABLE ... AS SELECT * FROM parquet_source`. This
rewrites the data once. Budget for disk + compute accordingly.

### Do this

1. Run the script the skill produces against non-prod.
2. Run `/iceberg-info` and confirm the snapshot count, total record
   count, and file count look right. The record count must match your
   Parquet source within the tolerance noted in the skill's output (zero
   if `add_files` was used).
3. Run a read query against the new Iceberg table from Athena or Spark
   and compare a sample of rows to the Parquet source.
4. Run against prod.

### Before you move on

- [ ] Iceberg table row count == Parquet source row count.
- [ ] A sampled read returns identical data.
- [ ] At least one snapshot exists and its manifest list references
      files on S3 that actually exist.

---

## Phase 4 — Add an Iceberg write alongside the existing Parquet write

**Run:** `/iceberg-data` (targeted change) OR `/iceberg-pipeline` (if
your job is small enough that a clean rewrite is easier).

This phase sets up **dual-writing**: every run lands the same batch in
BOTH the existing Parquet location AND the new Iceberg table. The
Parquet write stays in place; you are adding a second write, not
replacing the first. You will remove the Parquet write in Phase 5 once
reconciliation is clean.

### What it will ask you

- Tech stack (AWS Glue PySpark).
- Whether the write is append, overwrite, or upsert (MERGE). A Spark
  staging→conformed job is usually **append** unless you de-duplicate on
  a key, in which case it is **upsert**.
- The table name from Phase 2.

### What it produces

- A new write statement to drop into your job, right after (not
  replacing) the existing Parquet write. For append:
  `df.writeTo("glue_catalog.brok_conformed.<dataset>").append()`.
  For upsert: a `MERGE INTO` statement keyed on your business key.
- Spark-session configuration snippet for the Iceberg catalog (five
  `spark.sql.catalog.glue_catalog.*` keys).
- Updated Glue job parameters: `--datalake-formats iceberg` must be
  present. The skill will flag this if it is missing.
- A note about `job.commit()` placement: it must stay on the success
  path, **not** in a `finally` block. Calling it after a failure
  advances the Glue bookmark and the next run will skip unprocessed
  input.

### Do this

1. Apply the diff to a **branch** of your pipeline repo -- not main.
   The final script writes to Parquet first, then to Iceberg, both in
   the same transform pass so the two see identical input.
2. Run against non-prod. Verify both the Parquet location and the
   Iceberg table grew by the expected row count.
3. Deploy the branch to prod when non-prod looks good. You are now
   dual-writing in production -- proceed to Phase 5 for validation and
   cutover.

---

## Phase 5 — Dual-write validation and cutover

For some agreed window (usually 2–7 days in prod), your pipeline writes
to **both** the old Parquet location and the new Iceberg table. Every
run, you reconcile.

### Reconciliation each run

Write a small Spark reconciliation script (one-time effort, reused for
the whole window) that does three checks against the partition window
that just ran:

1. Row count: `SELECT COUNT(*)` against the Iceberg table, same
   against the Parquet source, for the same partition predicate.
   Deltas should be zero.
2. Column-level aggregates: `SELECT SUM(<numeric_col>), COUNT(DISTINCT
   <key_col>)` against both sides. Deltas should be zero.
3. Spot-check by key: sample 100 keys at random, read both sides,
   compare row-for-row.

Use `/iceberg-info` to inspect the Iceberg-side state (latest snapshot
id, row count, partitions touched) if you need to confirm what the
Iceberg write actually committed. `/iceberg-info` is for inspection;
the reconciliation logic itself is a short ad-hoc script you own.

If the diff is clean for the entire window, continue. If a diff appears:

- Note the partition values where it appeared.
- Do **not** remove the Parquet write path.
- Open the Phase 1 plan's rollback section.

### Cutover

Once the window is clean:

1. Remove the original Parquet write from the pipeline, keeping the
   Iceberg write that you added in Phase 4. Open a branch, delete the
   Parquet write block, and put the PR up.
2. Deploy and monitor one full run.
3. Update downstream consumers (Athena views, Redshift Spectrum
   references) to point at the Iceberg table. Athena v3 handles Iceberg
   v2 directly; older engines flagged in Phase 1 may need explicit
   upgrade.
4. Leave the Parquet files in place for now -- they are your rollback if
   anything surfaces in the first week post-cutover.

### Before you move on

- [ ] Pipeline has only one write path (Iceberg).
- [ ] At least one full day post-cutover produced no reconciliation
      deltas and no downstream complaints.
- [ ] You have a dated note to revisit deleting the old Parquet files
      after a retention window you pick (typically 30 days).

---

## Phase 6 — Schedule ongoing maintenance

**Run:** `/iceberg-maintenance`

An Iceberg table without maintenance degrades: snapshots accumulate,
small files appear, metadata grows. This skill produces a Glue job that
you schedule nightly.

### What it will ask you

- Tech stack (Glue PySpark).
- Retention policy for snapshots. A typical producer picks: keep the
  last 3 snapshots AND keep anything newer than 7 days.
- Orphan-file cleanup retention -- **must be ≥ 3 days** to avoid deleting
  in-progress commit files. Leave at the default 7 days unless you have
  a reason.

### What it produces

A single Glue Python script that runs the operations in the correct
order:

1. `CALL system.expire_snapshots(table => ..., older_than => ..., retain_last => 3)`
2. (If the table has delete files) `CALL system.rewrite_position_delete_files(...)`.
   The script queries the `.files` metadata table first and skips this
   call when there are no delete files -- correct for v1 tables and for
   v2 tables that have never had deletes.
3. `CALL system.rewrite_data_files(table => ..., target-file-size-bytes => 134217728)`
   to compact small files into 128 MB files.
4. `CALL system.rewrite_manifests(table => ...)` to tidy manifest
   distribution.
5. `CALL system.remove_orphan_files(table => ..., older_than => ...)` to
   clean up true orphans (partial writes from failed commits that no
   snapshot references).

Note: step 5 is still needed even though `expire_snapshots` deletes
files referenced only by expired snapshots. `expire_snapshots` does not
see true orphans.

### Do this

1. Deploy the script as a new Glue job named `brok-iceberg-maintenance`.
2. Schedule it to run daily via EventBridge or Glue Triggers at a low-
   traffic hour (your pipeline's off-peak).
3. First run: let it run manually, read the logs, confirm all five
   steps completed. You should see a snapshot expire, some files
   rewritten, and zero orphan files removed (unless you had failed
   commits in the past).

### Before you move on

- [ ] Maintenance job is scheduled.
- [ ] First manual run completed with no errors.
- [ ] `/iceberg-info` shows snapshot count within your retention policy.

---

## Phase 7 — Add the DR region

**Run:** `/iceberg-multi-region`

This is the step that gives you cross-region resilience under the
platform's constraint: **no cross-region S3 access, no Multi-Region
Access Points**. The pattern is:

1. S3 Cross-Region Replication (CRR) copies data and metadata objects
   from the primary bucket to a bucket in the DR region.
2. A small Lambda (or ECS task) reads the replicated `metadata.json` and
   rewrites every S3 path from the primary bucket to the DR bucket --
   and does the same for the manifest list and manifest Avro files.
3. The repointed metadata is written to the DR bucket.
4. The DR region's Glue Catalog is updated to point at the repointed
   `metadata.json`.

### What it will ask you

- Primary region (where your pipeline writes today).
- DR region.
- Whether the DR region is **read-only** (DR / reporting) or will be
  **promoted to primary** on failover (active-passive). Both are
  supported; the failover runbook below assumes active-passive.
- The DR bucket name. Convention: `brok-conformed-<dr-region>`.

### What it produces

- **Terraform** for S3 CRR: primary bucket versioning (already enabled
  in Phase 0), destination bucket with versioning, replication role,
  replication rule covering the `warehouse/` prefix, and delete-marker
  replication **disabled** (the skill and the multi-region planner
  agent both enforce this -- replicating delete markers in a DR context
  causes silent data loss on failover). The same Terraform package
  also deploys the repointer Lambda and its EventBridge trigger.
- **The metadata repointer** as a Lambda (Python) or ECS task (Java).
  The Python version uses `fastavro` to rewrite the manifest-list and
  manifest Avro files; both language versions preserve the Iceberg
  file-level Avro metadata headers (e.g. `iceberg.schema`), without
  which Iceberg readers reject the manifest.
- **EventBridge sync automation**: when CRR finishes replicating a new
  `metadata.json`, the repointer Lambda fires, repoints, and updates
  the DR Glue Catalog.
- **DR Glue Catalog registration** with `catalog.registerTable(...)`
  using the repointed metadata. The skill includes a rollback-on-
  failure path so that the DR catalog is never left empty if a
  re-register call fails.
- A **monitoring** bundle: CloudWatch alarm on CRR lag > 5 min, alarm
  on repointer Lambda failures, alarm on growing DR-vs-primary snapshot
  drift.

### Do this

1. Apply the Terraform in a **non-prod** account first.
2. Wait for CRR to seed the DR bucket (for large warehouses this can
   take hours on the first pass -- CRR is asynchronous).
3. Trigger the repointer manually on the most recent `metadata.json`
   key. Watch the Lambda logs.
4. Confirm in the DR Glue Catalog: the table exists, type = ICEBERG,
   `metadata_location` points to a `*.repointed.metadata.json` in the
   DR bucket.
5. From the DR region, run a read query (Athena or Spark) against the
   DR table. The rows should match the primary -- subject to CRR lag.
6. Apply the Terraform to prod.

### Before you move on

- [ ] CRR is replicating the primary bucket to the DR bucket with a lag
      you are comfortable with (alarm threshold fires appropriately in
      a test).
- [ ] Repointer runs on every new primary snapshot.
- [ ] DR Glue Catalog table reads correctly from the DR region.
- [ ] Monitoring alarms exist and route to your on-call channel.

---

## Phase 8 — Region failover flip runbook

Rehearse this in non-prod **before** you need it in prod. A real failover
is not the time to learn the commands.

### Pre-flip state

- Primary region: `us-east-1`, bucket `brok-conformed`, Glue table
  `brok_conformed.<dataset>` registered in `us-east-1`.
- DR region: `us-west-2`, bucket `brok-conformed-us-west-2`, Glue table
  `brok_conformed.<dataset>` registered in `us-west-2`, pointing at a
  `*.repointed.metadata.json` in that bucket.
- Pipeline is writing to the primary.

### Flip procedure (primary → DR)

1. **Stop writes on the primary**. Disable the Glue job schedule; if a
   run is in progress, let it finish or kill it. Confirm from CloudWatch
   metrics that no new commits are happening on the primary table.

2. **Drain CRR**. Check the CRR CloudWatch metric
   `ReplicationLatency` on the primary bucket. Wait until it reports
   zero pending objects, or until your SLA-acceptable lag has elapsed.

3. **Trigger one final repoint**. Invoke the repointer Lambda manually
   on the primary's current `metadata.json` key. This guarantees the DR
   catalog is pointing at the latest metadata, regardless of whether
   EventBridge lag delayed the automatic trigger. Confirm the DR Glue
   table's `metadata_location` matches the expected repointed key.

4. **Verify DR read**. From the DR region, run
   `SELECT COUNT(*), MAX(<timestamp_column>) FROM
   brok_conformed.<dataset>` against the DR table. The max timestamp
   should be the latest one your pipeline wrote on the primary before
   step 1.

5. **Flip the writer**. You should already have a disabled-but-deployed
   copy of your Glue job in the DR region, parameterized so that its
   `warehouse` config points at `s3://brok-conformed-us-west-2/
   warehouse/` and its Spark catalog config points at the DR region's
   Glue Catalog. (If you do not, deploy it now -- it is the same
   pipeline code from Phase 4 with region-specific environment
   variables.) Enable its schedule.

6. **Verify writes**. Let one run complete. Confirm the DR table grew,
   and that the pipeline's CloudWatch logs show a successful commit.
   Note: writes made in the DR region after the flip go natively into
   the DR bucket and DR Glue Catalog -- **they do not need repointing**,
   because Iceberg writes whatever paths the writer session configures
   and the writer is now configured for DR. Repointing only applies to
   metadata that was originally written in a different region and
   copied in by CRR.

7. **Announce**. The DR region is now primary. Update runbooks and
   dashboards.

### Flip back (DR → primary)

The symmetric procedure, plus one extra step: you may need to re-seed
CRR the other way (from the new primary back to the old primary) if
the old primary was offline long enough to accumulate missing objects.
A clean way to do this:

1. Stop writes on the DR-acting-as-primary.
2. Use AWS DataSync or `aws s3 sync` to seed the missing objects from
   DR back to the original primary bucket.
3. Trigger the repointer on the original primary against its latest
   metadata key (now synced from DR).
4. Verify read in the original primary.
5. Flip the writer back.
6. Re-enable CRR from original primary → DR.

If this sounds tedious -- it is. Many teams instead declare the DR
region the new primary after a failover and set up CRR in the reverse
direction as the new steady state. Make a conscious choice; do not
default into it.

### Never do this

- **Never write to both regions simultaneously**. Iceberg assumes a
  single writer per table. Concurrent commits across regions, with no
  cross-region catalog to coordinate, will split the table into
  divergent snapshot histories that no amount of repointing can
  reconcile.
- **Never skip step 2 (drain CRR)**. Flipping while CRR is behind will
  silently drop the most recent committed data.
- **Never run the repointer against partially replicated metadata.**
  If CRR is behind, some manifest files referenced by
  `metadata.json` may not yet exist in the DR bucket. The repointer
  will succeed (it does not verify manifest existence), but reads
  from the DR table will fail. Step 2 protects against this.

---

## Appendix A — Rollback by phase

| Rolled back from | How |
|-|-|
| Phase 2 (table created) | Run `DROP TABLE glue_catalog.brok_conformed.<dataset>` in Spark. No user data touched; only the catalog entry and empty Iceberg metadata are removed. |
| Phase 3 (backfill via `add_files`) | `DROP TABLE ...` (no `PURGE` keyword). Iceberg removes its catalog entry and metadata; the registered Parquet files remain in S3 because `add_files` referenced them in place. |
| Phase 3 (backfill via CTAS) | `DROP TABLE ... PURGE` -- the `PURGE` keyword deletes the CTAS-written data. Original Parquet source is untouched. |
| Phase 4 (pipeline changed on branch) | Abandon the branch. Production still writes Parquet. |
| Phase 5 (cutover done) | Re-enable the Parquet write path in a branch. Restore from the Parquet files that still exist (you kept them per Phase 5's final checklist). |
| Phase 6 (maintenance) | Disable the maintenance Glue job's schedule. Pipeline is unaffected. |
| Phase 7 (multi-region) | Keep CRR running for data safety; disable the DR region's Glue table by running `DROP TABLE` (no `PURGE`) in the DR region's Spark. The replicated data stays in the DR bucket. |
| Phase 8 (failover) | Flip back -- see the runbook. |

---

## Appendix B — Troubleshooting

**Symptom:** `/iceberg-ddl` script fails with `Unknown type timestamp(ns)`.

The schema inferred from your Parquet files has nanosecond-precision
timestamps. Iceberg format-v2 stores microseconds. Downcast first; see
Phase 3's migrate skill, which includes the downcast helper.

---

**Symptom:** After Phase 3, Athena reads return NULL for columns that
exist in the Parquet files.

The name-mapping was not registered. Re-run the migrate skill -- it sets
`schema.name-mapping.default` as a table property, which allows Iceberg
to resolve columns by name for Parquet files without embedded Iceberg
field IDs.

---

**Symptom:** Glue job fails on first Iceberg write with
`Table does not exist: glue_catalog.brok_conformed.<dataset>`, but the
table is visible in the Glue console.

`--datalake-formats iceberg` is missing from the Glue job parameters.
Both this flag **and** the explicit `spark.sql.catalog.glue_catalog.*`
configs in the Spark session are required; they are complementary, not
redundant.

---

**Symptom:** Maintenance job run time is growing every night.

Expected during the first few runs while the backlog is worked down.
If it still grows after a week, check for two common causes:

- Orphan data files accumulating from failed commits. Run
  `/iceberg-info` and ask for the orphan file count. If the count is
  non-zero and climbing, investigate why writes are failing mid-commit
  (IAM, timeouts, spot-interrupt) before cranking up `rewrite_data_files`
  parallelism.
- Too many snapshots. Your `retain_last` / `older_than` settings may be
  keeping more history than the table actually needs. Tighten the
  policy and let expire_snapshots catch up.

---

**Symptom:** DR read returns stale data.

CRR lag. Check the CloudWatch CRR lag metric. The DR Glue table will
not reflect a new snapshot until (a) the `metadata.json` replicates,
(b) every manifest file it references replicates, and (c) the
repointer Lambda runs against it. In steady state all three happen
within minutes; a burst of writes can push lag into the tens of
minutes.

---

**Symptom:** DR Glue table is empty after a re-register.

The drop+register window caught a transient Glue error on the register
call. The repointer's rollback path should have re-registered against
the previous metadata -- check the Lambda logs. If it did not, run the
repointer manually against the last-known-good `metadata.json` key.

---

## Appendix C — One-page checklists

### Pre-flight (before you touch anything)

- [ ] Glue 5.0 or 5.1 job runtime
- [ ] IAM role has S3 and Glue Catalog permissions
- [ ] Conformed bucket versioning ENABLED
- [ ] `--enable-glue-datacatalog true` in job parameters
- [ ] Non-prod database and dataset available for rehearsal
- [ ] Phase 1 plan reviewed and partition spec agreed with stakeholders

### Post-flight (ready to call it done)

- [ ] Iceberg table exists, format-version = 2
- [ ] Pipeline writes only to Iceberg, no reconciliation deltas for 7 days
- [ ] Maintenance job scheduled and first run clean
- [ ] DR region registered, CRR seeded, repointer firing on new snapshots
- [ ] Failover runbook rehearsed end-to-end in non-prod
- [ ] Downstream consumers migrated to the Iceberg table
- [ ] Dated note to retire old Parquet files after 30 days

---

## Quick reference — which skill does what

| Skill | When to use it |
|-|-|
| `/iceberg-onboard` | Phase 1 only. Produces the migration plan. |
| `/iceberg-ddl` | Phase 2. Create the table. Also: schema changes post-cutover. |
| `/iceberg-migrate` | Phase 3. One-time backfill. |
| `/iceberg-pipeline` | Phase 4 (full rewrite of the job) or a greenfield producer. |
| `/iceberg-data` | Phase 4 (targeted write-path change) and any future ingest tweaks. |
| `/iceberg-info` | Phases 2, 3, 5, 6 -- verification at every checkpoint. |
| `/iceberg-maintenance` | Phase 6. Also: ad-hoc compaction requests. |
| `/iceberg-multi-region` | Phase 7. Also: adding more DR regions later. |

If you are unsure which to run, `/iceberg-onboard` is always the safe
starting point -- it delegates to the right specialist.
