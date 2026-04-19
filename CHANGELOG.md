# Changelog

All notable changes to DMIceberg are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — 2026-04-19

Initial product release. Scope: AWS Glue Data Catalog only. Storage: S3 only. Iceberg format-version 2.

### Skills

- `/iceberg-profile-setup` — one-time onboarding captures producer conventions into `profiles/<producer-name>.yaml`
- `/iceberg-onboard` — assessment + migration strategy (delegates to `iceberg-architect`)
- `/iceberg-ddl` — create / alter / drop tables with schema evolution
- `/iceberg-data` — insert, upsert (MERGE), delete, overwrite; batch and streaming
- `/iceberg-migrate` — backfill from existing Parquet or Hive (`add_files`, CTAS, `snapshot`)
- `/iceberg-maintenance` — compaction, snapshot expiration, orphan cleanup
- `/iceberg-multi-region` — S3 CRR + metadata repointing + DR-catalog registration
- `/iceberg-pipeline` — end-to-end pipeline scaffolding for greenfield producers
- `/iceberg-info` — metadata inspection and diagnostics

### Subagents

- `iceberg-orchestrator` — end-to-end coordinator
- `iceberg-architect` — setup analysis + migration design
- `iceberg-code-generator` — stack-specific code generation
- `iceberg-validator` — correctness checks on configs, schemas, generated code
- `iceberg-multi-region-planner` — multi-region architecture planning

### Tech-stack coverage

- Glue PySpark (`iceberg-spark-runtime`)
- EMR PySpark (`iceberg-spark-runtime`)
- ECS Python (`pyiceberg`)
- Lambda Python (`pyiceberg`)
- ECS Java (`iceberg-core` + `iceberg-aws`)
- Lambda Java (`iceberg-core` + `iceberg-aws`)

### Profiles

- `profiles/schema.md` — profile schema reference
- `profiles/presets/aws-glue-datamesh-strict.yaml` — strict multi-region isolation (no cross-region S3, no MRAPs)
- `profiles/presets/aws-glue-central-lake.yaml` — shared-bucket permissive (cross-region S3 allowed)

### Documentation

- `docs/using-dmiceberg.md` — generic user guide (phase model, skills, agents, multi-region, rollback, troubleshooting)
- `docs/admin-guide.md` — platform-team guide (operating model, profiles, gating, rollout, observability, incidents)
- `docs/examples/pyspark-producer-recipe.md` — Glue/EMR PySpark recipe
- `docs/examples/pyiceberg-producer-recipe.md` — ECS/Lambda Python recipe
- `docs/examples/java-producer-recipe.md` — ECS/Lambda Java recipe

### IDE compatibility

- Claude Code: full support (skills + agents + orchestration)
- GitHub Copilot: skills auto-discovered from `.claude/skills/` (subagent orchestration is Claude-only)

### Pre-release correctness fixes (applied before tag)

Prior to cutting `1.0.0` a full review pass was run across every skill, subagent, and doc. The following correctness issues were found and fixed — documented here so producers who saw earlier drafts know what changed:

- **PyIceberg API alignment.** Snapshot expiry now uses `table.expire_snapshots().expire_older_than(epoch_ms).commit()` (the actual 0.8+ API); the pre-release draft referenced a nonexistent `tx.expire_snapshots_older_than(datetime)`. Name mapping for `add_files` now uses `create_mapping_from_schema(...)` + `NameMapping.model_dump_json()` instead of an unsupported `model_dump()`/`json.dumps` wrap.
- **Glue job lifecycle.** `iceberg-maintenance` and `iceberg-migrate` Glue templates now `Job.init(...)` / `job.commit()` on the success path instead of calling `spark.stop()`. This prevents the Glue bookmark from advancing on partial failures.
- **Java writer pattern.** `iceberg-data` and `iceberg-pipeline` Java examples now call `writer.toDataFile()` only after a successful `writer.close()`, with a catch block that deletes the orphan data file and re-throws. The prior `finally`-based pattern could commit a partial file or call `toDataFile()` on a writer that threw in `close()`.
- **PySpark type comparisons.** `iceberg-migrate` type checks now use `isinstance(dtype, (LongType, ...))` instead of string comparisons (`dtype == "long"`), which never matched a `DataType` object.
- **Region pinning for Java catalog.** The canonical Java template in `iceberg-code-generator` now requires `glue.region` + `s3.region` on catalog init. Without these, the AWS SDK resolves region from env/IMDS/profile, which in multi-region deployments silently triggers cross-region S3 access — explicitly disallowed on this platform.
- **IAM Glue scope.** Generated Terraform in `iceberg-pipeline` no longer grants `glue:*` on `Resource = "*"`. Glue actions are now scoped to `arn:aws:glue:REGION:ACCOUNT:catalog`, `database/<db>`, and `table/<db>/*`. Only `lakeformation:GetDataAccess` retains wildcard scope — it does not support resource-level IAM and is enforced via Lake Formation grants instead.

### Known limitations

- v1 supports only AWS Glue Data Catalog. Unity Catalog, Polaris, Nessie, and Hive Metastore are not in scope.
- Existing skills do not yet read profile placeholders; they still ask for bucket/database inline. Profile-aware skill refactor tracked for a subsequent minor release.
- `.github/copilot-instructions.md` not shipped; Copilot users rely on skill frontmatter + recipe files.
