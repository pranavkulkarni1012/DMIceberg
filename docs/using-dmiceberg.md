# Using DMIceberg

DMIceberg is a set of Claude Code skills and subagents that guide Data Mesh producers through migrating Parquet pipelines to Apache Iceberg on AWS, with multi-region resilience built in.

This is the **generic product guide**. It is tech-stack-agnostic — everything here applies whether you run PySpark on Glue, PySpark on EMR, PyIceberg on ECS, PyIceberg on Lambda, Java on ECS, or Java on Lambda. For copy-pasteable examples in your specific stack, go to a recipe under [`docs/examples/`](./examples/) after reading this guide.

---

## 1. What DMIceberg gives you

- **8 skills** (slash commands) that cover the full lifecycle of an Iceberg producer: profile setup, onboarding, table creation, data operations, migration from existing Parquet, maintenance, multi-region resilience, pipeline scaffolding, and metadata inspection.
- **5 specialist subagents** that handle deeper analysis and code generation. Claude Code users get the orchestrated experience where agents coordinate with each other. GitHub Copilot users get the skills directly — Copilot auto-discovers `.claude/skills/` but does not dispatch subagents.
- **3 tech-stack recipes** showing concrete end-to-end flows for PySpark, PyIceberg, and Java producers.
- **Tenant profiles** — YAML files that capture your organization's conventions (buckets, regions, orchestrator, multi-region rules) so you answer them once, not per skill invocation.

## 2. Scope of v1

Locked in:

- **Catalog:** AWS Glue Data Catalog only
- **Storage:** Amazon S3 only
- **Table format:** Apache Iceberg, format-version 2
- **Data formats:** Parquet (primary), with migration paths from existing Parquet and Hive tables

Flexible:

- **Compute:** Glue, EMR, ECS, Lambda
- **Language/library:** PySpark (`iceberg-spark-runtime`), Python (`pyiceberg`), Java (`iceberg-core` + `iceberg-aws`)
- **Orchestrator:** Step Functions, MWAA/Airflow, Glue Workflows, Dagster — whatever you already use
- **Region pair:** any AWS region pair (`us-east-1`/`us-west-2`, `eu-west-1`/`eu-central-1`, `ap-southeast-1`/`ap-southeast-2`, etc.)

## 3. Platform constraints

These are **opinionated defaults** baked into the `aws-glue-datamesh-strict` profile. If any of these don't match your environment, pick a different profile or build a custom one (see [`docs/admin-guide.md`](./admin-guide.md)).

1. **Each producer owns its own S3 bucket.** No shared producer buckets.
2. **Cross-region S3 access is not allowed.** Producers in region A cannot read from a bucket in region B. Every region has its own bucket with locally resident data.
3. **Multi-Region Access Points (MRAPs) are not allowed.** Compliance-driven restriction at many large enterprises; we replicate + repoint instead.
4. **S3 Cross-Region Replication (CRR)** is used for data + metadata replication between regions.
5. **Iceberg metadata is repointed**, not cross-region-read. The repointing utility rewrites every S3 URI in `metadata.json`, manifest-list Avros, and manifest Avros from source bucket to target bucket.
6. **Tables are registered in each region's Glue Catalog independently.** A table that exists in `us-east-1` is a separate Glue Catalog entry in `us-west-2`, pointing to the repointed metadata.
7. **Warehouse path convention:** `s3://<producer-bucket>/warehouse/`.

## 4. The 8-phase lifecycle

Every producer migration follows these phases. Skip phases that don't apply (e.g., skip phase 3 if you're starting fresh with Iceberg rather than migrating).

| Phase | Name | Skill | Goal |
|-------|------|-------|------|
| 0 | Profile setup | `/iceberg-profile-setup` | Capture your org's conventions once |
| 1 | Assess | `/iceberg-onboard` | Discover your current pipeline, buckets, orchestrator |
| 2 | Create | `/iceberg-ddl` | Define the target Iceberg schema and partitioning |
| 3 | Backfill | `/iceberg-migrate` | Move existing Parquet/Hive data into Iceberg |
| 4 | Dual-write | `/iceberg-data` | Add Iceberg writes alongside the existing Parquet pipeline |
| 5 | Cutover | (manual) | Remove the Parquet write; Iceberg becomes the sole target |
| 6 | Maintain | `/iceberg-maintenance` | Compaction, snapshot expiry, orphan cleanup |
| 7 | Multi-region | `/iceberg-multi-region` | CRR + metadata repointing + DR catalog registration |
| 8 | Failover | (runbook) | Flip reads/writes to the DR region and back |

Phase 5 is intentionally manual — cutover is the highest-risk step and we want a human decision at that moment. Everything else is automatable.

## 5. Skill reference

### `/iceberg-profile-setup`
One-time onboarding. Walks you through ~12 questions and writes a `profile.yaml` for your producer into `profiles/`. Every other skill reads from this profile so you don't re-answer region/bucket/orchestrator questions.

### `/iceberg-onboard`
Discovery of your current producer state. Reads your pipeline code, S3 layout, and Glue Catalog entries. Produces a migration strategy document tailored to your stack. Internally delegates to the `iceberg-architect` subagent.

### `/iceberg-ddl`
Creates, alters, and drops Iceberg tables. Handles schema evolution (add/rename/drop columns, widen types, partition spec evolution). Generates the appropriate code for your stack (Spark SQL, PyIceberg, Java API).

### `/iceberg-data`
Inserts, upserts (`MERGE INTO`), deletes, and partition overwrites. Supports batch and streaming patterns. Decides merge-on-read vs copy-on-write based on your read/write ratio.

### `/iceberg-migrate`
Converts existing Parquet or Hive tables to Iceberg. Three modes: in-place with `system.migrate` (Spark), snapshot with `system.snapshot` (non-destructive), or file-level registration with `system.add_files`. For non-Spark stacks, generates equivalent PyIceberg `add_files()` flows. Adds `schema.name-mapping.default` to read Parquet files without embedded field IDs.

### `/iceberg-maintenance`
Three maintenance operations: compaction (`rewrite_data_files` + `rewrite_position_delete_files`), snapshot expiration (`expire_snapshots`), and orphan file cleanup (`remove_orphan_files`). Generates scheduled-job code suitable for your orchestrator.

### `/iceberg-multi-region`
Builds the multi-region DR pipeline: S3 CRR setup, metadata repointing utility (Python or Java), DR-region Glue Catalog registration, sync automation, monitoring, and failover runbook. Delegates to `iceberg-multi-region-planner`.

### `/iceberg-pipeline`
End-to-end pipeline scaffolding for new producers. Stitches together ingestion → Iceberg write → maintenance schedule → multi-region sync in one generated project. Good for greenfield; existing producers usually use the individual skills.

### `/iceberg-info`
Metadata inspection: snapshot history, manifest stats, file counts, partition distribution, delete-file ratios. Useful during debugging and before maintenance runs.

## 6. Subagent reference

Subagents are invoked via Claude Code's Task tool (Claude-only). Copilot users do not have access to subagent dispatch; the skills work standalone.

| Agent | Role |
|-------|------|
| `iceberg-orchestrator` | End-to-end coordinator. Invoked by `/iceberg-onboard`. Delegates to the four specialists below in sequence. |
| `iceberg-architect` | Analyzes producer setup, identifies migration risks, designs the table/partition strategy. |
| `iceberg-code-generator` | Produces stack-specific code (Spark/PyIceberg/Java) from a spec. |
| `iceberg-validator` | Runs correctness checks on generated code, configs, and schemas. Flags common pitfalls (missing `s3.region`, ns-timestamp schema mismatches, `region_name` typos, etc.). |
| `iceberg-multi-region-planner` | Plans the CRR + repointing + DR-catalog architecture. Generates the sync + failover automation. |

## 7. Multi-region architecture

The flow, in one diagram's worth of prose:

1. **Primary region writes.** Producer job writes Iceberg data + metadata to `s3://producer-bucket-primary/warehouse/<db>/<table>/`. Table is registered in primary-region Glue Catalog.
2. **CRR replicates.** S3 Cross-Region Replication copies every new object (data files, delete files, manifest Avros, manifest-list Avros, `metadata.json`, version-hint) to `s3://producer-bucket-dr/warehouse/<db>/<table>/`.
3. **Metadata is broken at this point.** The replicated `metadata.json` still references the primary bucket URI. A reader in the DR region cannot follow those pointers (cross-region S3 is blocked).
4. **Repointing utility runs in the DR region.** It reads the replicated `metadata.json`, rewrites every `location` / path field from `producer-bucket-primary` to `producer-bucket-dr`. It then reads each referenced manifest-list Avro, rewrites its paths, writes it back. Same for each manifest Avro. Avro file-level metadata (`avro.codec`, `iceberg.schema`, etc.) is preserved.
5. **DR-region Glue Catalog registration.** The repointed metadata is registered as a table in the DR-region Glue Catalog, pointing to the repointed `metadata.json`.
6. **Sync schedule.** Repointing runs on a schedule (typical: every 15 minutes) to keep DR current.

### What the repointing touches

- `metadata.json` → always
- `snap-<id>-<seq>-<uuid>.avro` (manifest list) → always
- `<uuid>-m0.avro` (manifest) → always
- Data files (`.parquet`) → never; only the references to them in manifests change
- Delete files (`.parquet` for equality deletes, `.parquet` for positional deletes) → never; only references change

### What CRR does not replicate

- Glue Catalog entries — those are catalog service state, not S3 state. The repointing utility creates the DR-region Glue entry.
- Lake Formation permissions — re-grant explicitly in DR region.
- Anything in a different bucket (e.g., the Iceberg warehouse in a different bucket than your replication source).

## 8. Failover principles

A failover flip is a **read-path change** first and a **write-path change** second. In order:

1. **Stop writes to primary.** Producer pipeline pauses.
2. **Drain in-flight writes.** Wait for CRR to catch up. Use CRR metrics (`ReplicationLatency`, `BytesPendingReplication`).
3. **Run repointing once more.** Ensure the DR region has the latest metadata.
4. **Flip readers.** Point consumer jobs at the DR-region Glue Catalog table.
5. **Flip writers.** Producer pipeline resumes, now writing to the DR-region bucket natively (no repointing needed — DR writes are first-class, not replicated-and-repointed).
6. **Reverse CRR.** During DR-active, enable CRR from DR bucket back to primary so you can fail back later.

### Failback principles

Same flow in reverse. **Never skip the repointing step on failback** — your primary region's metadata is stale and still references itself pre-failover, but the data/snapshots written during DR-active need to be repointed when the primary comes back up.

### What you do not do

- **Do not write to both regions simultaneously.** Iceberg's catalog doesn't support multi-writer coordination across regions in this model. Writes go to exactly one region at a time.
- **Do not try to read cross-region.** Readers always use their local-region Glue Catalog + local-region bucket.
- **Do not manually edit `metadata.json`.** Use the repointing utility, even for small fixes — hand-edits break Avro manifest references.

## 9. Rollback principles

Every phase has a rollback. The generic rules:

- **Phase 0-2 (profile, assess, DDL):** No data touched yet. Rollback is deleting the profile or dropping the empty table.
- **Phase 3 (backfill):** Use `/iceberg-ddl` to drop the table, or `/iceberg-maintenance` to expire all snapshots back to the pre-backfill state if you used `system.snapshot` (non-destructive migration).
- **Phase 4 (dual-write):** Simplest rollback — remove the Iceberg write block from your pipeline; Parquet pipeline is untouched.
- **Phase 5 (cutover):** Highest-risk rollback. You must restore the Parquet write and accept that new data written to Iceberg since cutover will be dual-maintained for the reconciliation window. Plan a reconciliation window of 2-4 weeks post-cutover where you can fall back to Parquet if issues emerge.
- **Phase 6 (maintenance):** Snapshot expiration is irreversible once expired snapshots are pruned. Compaction rewrites are reversible via snapshot rollback (`CALL system.rollback_to_snapshot`).
- **Phase 7 (multi-region):** Disable CRR + drop the DR-region Glue entry. Data remains in DR bucket; repoint only runs in DR. Stopping it leaves primary untouched.

## 10. Troubleshooting principles

Common symptoms and their usual causes:

| Symptom | Likely cause | Where to look |
|---------|--------------|---------------|
| `NoSuchTableException` after create | Glue database missing, or region mismatch between catalog and write | Glue Console; verify `glue.region` / `s3.region` keys |
| Reads fail after multi-region repoint | Manifest Avro references still point to primary bucket | Re-run repointing; check `iceberg.schema` metadata preserved in Avro rewrite |
| Slow reads on Iceberg table | Too many small files, or too many delete files (MoR) | `/iceberg-info` → run `/iceberg-maintenance` compaction |
| `CommitFailedException` on concurrent write | Two writers racing on the same snapshot | Add retry with exponential backoff; separate writers by partition |
| `add_files` rejects files | Duplicate files already in the table | Set `check_duplicate_files => false` if you've verified it's safe |
| Parquet field not found after migrate | Missing `schema.name-mapping.default` | Add the name-mapping property to the table |
| Timestamp column type mismatch | Source is ns-precision, Iceberg v2 is us-precision | Downcast with the helper in the migrate skill |
| `region_name` error in PyIceberg | Outdated argument name | Use canonical `glue.region` and `s3.region` |

## 11. What to do next

1. **Run `/iceberg-profile-setup`** to capture your producer's conventions.
2. **Pick a recipe** matching your stack:
   - [PySpark (Glue or EMR)](./examples/pyspark-producer-recipe.md)
   - [PyIceberg (ECS or Lambda Python)](./examples/pyiceberg-producer-recipe.md)
   - [Java (ECS or Lambda Java)](./examples/java-producer-recipe.md)
3. **Follow the 8-phase lifecycle** in your recipe. Each phase invokes the skill you need.
4. **Platform team?** Read [`docs/admin-guide.md`](./admin-guide.md) for rollout, governance, and multi-producer operations.

---

*v1.0 — AWS Glue Data Catalog only. Cross-cloud / Unity / Polaris support not in this release.*
