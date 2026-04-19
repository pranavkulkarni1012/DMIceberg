# Profile Schema

A **profile** captures the conventions and constraints of a Iceshelf deployment. Every skill reads the profile so producers don't re-answer the same questions per invocation.

Profiles live in `profiles/<producer-name>.yaml`. Presets live in `profiles/presets/` and are referenced via `extends:`.

---

## Resolution order

1. Load the producer's profile YAML.
2. If `extends:` is set, load that preset and merge it underneath the producer's fields (producer values win on conflict).
3. If the preset itself has `extends:`, recurse (up to 3 levels deep to prevent runaway inheritance).
4. Unspecified fields fall back to schema defaults documented below.

## Required fields

| Field | Type | Notes |
|-------|------|-------|
| `producer_name` | string | Must match the filename (minus `.yaml`). Lowercase, dash-separated. |
| `cloud` | enum | `aws` (only value in v1). |
| `catalog` | enum | `glue` (only value in v1). |
| `primary_region` | string | AWS region code, e.g. `us-east-1`. |
| `warehouse_bucket_primary` | string | S3 bucket name (no `s3://` prefix, no path). |
| `database` | string | Glue database name. |
| `tech_stack` | enum | See "tech_stack values" below. |
| `format_version` | integer | `2` default. `1` allowed only for migration-in-flight tables. |

## Optional fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `extends` | string | — | Name of a preset in `profiles/presets/`. |
| `dr_region` | string | — | Required if `multi_region.enabled`. |
| `warehouse_bucket_dr` | string | — | Required if `multi_region.enabled`. |
| `multi_region` | object | `{enabled: false}` | See "multi_region" below. |
| `orchestrator` | enum | `step_functions` | `step_functions`, `mwaa`, `airflow`, `glue_workflows`, `dagster`. |
| `partition_defaults` | object | See below | Table property defaults for new tables. |
| `maintenance` | object | See below | Schedules for compaction / expiry / orphan cleanup. |
| `kms` | object | — | Encryption config. |
| `lake_formation` | object | `{enabled: false}` | LF integration. |
| `governance` | object | — | Required tags, audit bucket. |
| `observability` | object | — | Metrics namespace, alarm thresholds. |
| `naming_conventions` | object | See below | Bucket / DB / table naming patterns. |

## `tech_stack` values

- `glue_pyspark` — AWS Glue 5.0/5.1, PySpark
- `emr_pyspark` — EMR 7.x, PySpark
- `ecs_python` — ECS Fargate, PyIceberg
- `lambda_python` — Lambda, PyIceberg
- `ecs_java` — ECS Fargate, iceberg-core + iceberg-aws
- `lambda_java` — Lambda, iceberg-core + iceberg-aws

## `multi_region` object

```yaml
multi_region:
  enabled: true
  cross_region_s3_allowed: false           # if true, skip repointing and read cross-region directly
  multi_region_access_points_allowed: false # if true, use MRAP instead of CRR+repoint
  repointing_schedule_minutes: 15
  failover_rto_minutes: 60
  failover_rpo_minutes: 20
```

When `cross_region_s3_allowed: false` and `multi_region_access_points_allowed: false`, the multi-region skill generates the CRR + repointing + local-Glue architecture. Any other combination generates a simpler architecture.

## `partition_defaults` object

```yaml
partition_defaults:
  format_version: 2
  target_file_size_bytes: 134217728        # 128 MB
  parquet_compression_codec: zstd           # snappy | gzip | zstd | lz4
  distribution_mode: hash                   # hash | range | none
  delete_mode: merge-on-read                # merge-on-read | copy-on-write
  metadata_delete_after_commit: true
  metadata_previous_versions_max: 10
```

## `maintenance` object

```yaml
maintenance:
  compaction:
    schedule: "0 0 * * *"                   # cron (daily midnight UTC)
    target_file_size_bytes: 134217728
    min_input_files: 5
  snapshot_expiration:
    schedule: "0 2 * * 0"                   # Sunday 02:00 UTC
    retain_days: 7
    retain_last: 10
  orphan_cleanup:
    schedule: "0 3 1 * *"                   # 1st of month, 03:00 UTC
    older_than_days: 14
```

## `kms` object

```yaml
kms:
  default_key_alias: alias/iceshelf-tables
  bucket_encryption: aws:kms                # aws:kms | AES256
  grantees:
    - arn:aws:iam::123456789012:role/producer-role
```

## `lake_formation` object

```yaml
lake_formation:
  enabled: true
  lf_tags:
    required: [data_classification, owner_team]
    defaults:
      data_classification: internal
```

## `governance` object

```yaml
governance:
  required_tags:
    - CostCenter
    - Owner
    - DataClassification
  audit_bucket: s3://org-audit-central/iceberg-events/
  change_approval:
    ddl_requires_ticket: true
    migrate_requires_ticket: true
```

## `observability` object

```yaml
observability:
  metrics_namespace: "Org/Iceshelf"
  alarms:
    commit_failure_threshold: 5
    commit_failure_window_minutes: 15
    repointing_latency_threshold_seconds: 1800
    snapshot_count_threshold: 10000
  dashboards:
    per_producer: true
    fleet_rollup: true
```

## `naming_conventions` object

```yaml
naming_conventions:
  bucket_pattern: "{producer_name}-{stage}-{region}"      # e.g. brok-confirmed-us-east-1
  database_pattern: "{producer_name}_{stage}"              # e.g. brok_confirmed
  warehouse_path: "s3://{bucket}/warehouse/"
```

These drive generated code. If not set, skills ask per invocation.

---

## Example: minimal producer profile

```yaml
# profiles/datamesh-producer-brok.yaml
producer_name: datamesh-producer-brok
extends: aws-glue-datamesh-strict
primary_region: us-east-1
dr_region: us-west-2
warehouse_bucket_primary: brok-confirmed-us-east-1
warehouse_bucket_dr: brok-confirmed-us-west-2
database: brok_confirmed
tech_stack: glue_pyspark
orchestrator: step_functions
```

## Example: greenfield with custom partitioning

```yaml
producer_name: datamesh-producer-nova
extends: aws-glue-datamesh-strict
primary_region: eu-west-1
warehouse_bucket_primary: nova-confirmed-eu-west-1
database: nova_confirmed
tech_stack: ecs_python
multi_region:
  enabled: false    # not yet; will turn on in phase 7

partition_defaults:
  delete_mode: copy-on-write    # read-heavy workload
  parquet_compression_codec: snappy   # smaller decompression cost
```

---

## Validation

When a profile is loaded, the `iceberg-validator` agent checks:

- Required fields present
- Region codes are valid AWS regions
- Bucket names match AWS naming rules
- `extends` chain resolves without cycle
- `dr_region` differs from `primary_region` if multi-region enabled
- `tech_stack` is a supported value
- Cron expressions in `maintenance` are parseable
- Warnings for non-canonical PyIceberg args (`region_name` instead of `glue.region`/`s3.region`)
