# DMIceberg Admin Guide

For platform teams deploying DMIceberg across many producers. This is **not** for individual producers — they follow their tech-stack recipe in [`docs/examples/`](./examples/).

---

## 1. Who this guide is for

You are the platform/data-infrastructure team responsible for:

- Rolling out Iceberg migration across N producer teams (10s or 100s)
- Standardizing profiles, naming conventions, and multi-region posture
- Governing who can run which skills in which environment
- Monitoring migrations at the fleet level (not per-producer)
- Handling incidents that producers escalate

Individual producer teams are your customers here. The skills they use are the ones you've configured.

## 2. Operating model

DMIceberg is deployed per-repository. There are two common patterns:

### Pattern A: Shared platform repo (recommended)

- Platform team owns a single DMIceberg repo
- Producers link it as a git submodule or subtree into their own repo
- Producer's CLAUDE.md points at the submodule location
- Platform team ships updates via PRs; producers pull updates on their cadence

**Pros:** one canonical source, fast rollouts of fixes. **Cons:** producers must re-run their migration with updated profile if schema changes.

### Pattern B: Per-producer copy

- Each producer copies DMIceberg into their own repo
- No automated updates — producers cherry-pick fixes

**Pros:** full autonomy. **Cons:** platform team's fixes never propagate; fleet drifts.

We recommend Pattern A for orgs with 10+ producers. Pattern B is acceptable for <5 producers where platform is informal.

## 3. Authoring a profile

A profile is a YAML file in `profiles/<producer-name>.yaml`. Schema is documented in [`profiles/schema.md`](../profiles/schema.md). As a platform team, you'll typically:

1. Fork one of the provided presets (`aws-glue-datamesh-strict`, `aws-glue-central-lake`) into `profiles/presets/<your-org>-<posture>.yaml`.
2. Encode your org's defaults: KMS key ARNs, VPC endpoint configs, Lake Formation usage, audit bucket, standard tag keys.
3. Publish the preset; producers reference it via `extends: <your-org>-<posture>` and override only producer-specific fields.

### Example org preset

```yaml
# profiles/presets/acme-corp-regulated.yaml
extends: aws-glue-datamesh-strict

cloud: aws
catalog: glue
cross_region_s3_allowed: false
multi_region_access_points_allowed: false

kms:
  default_key_alias: alias/acme-iceberg-tables
  bucket_encryption: aws:kms

lake_formation:
  enabled: true
  lf_tags:
    required: [data_classification, owner_team, retention_tier]

governance:
  required_tags:
    - CostCenter
    - Owner
    - DataClassification
  audit_bucket: s3://acme-audit-central/iceberg-events/

observability:
  metrics_namespace: "ACME/DMIceberg"
  alarms:
    commit_failure_threshold: 5          # alarm if >5 CommitFailedException per 15min
    repointing_latency_threshold_seconds: 1800
```

Producers then write a thin profile:

```yaml
# profiles/datamesh-producer-brok.yaml
extends: acme-corp-regulated
producer_name: datamesh-producer-brok
primary_region: us-east-1
dr_region: us-west-2
warehouse_bucket_primary: brok-confirmed-us-east-1
warehouse_bucket_dr: brok-confirmed-us-west-2
database: brok_confirmed
orchestrator: step_functions
tech_stack: glue_pyspark
```

## 4. Gating skills per team

Claude Code supports per-directory allowlisting. Use `.claude/settings.local.json` (or team settings) to restrict which skills each environment can run.

**Dev environment:** all skills available.
**Staging:** all skills except `/iceberg-multi-region`, `/iceberg-migrate` (destructive operations gated).
**Prod:** only `/iceberg-data`, `/iceberg-maintenance`, `/iceberg-info` available; migrations go through a change-management ticket.

Document this in your org preset's README so producers know what's allowed where.

## 5. Rollout strategy

### Wave 1: Pilot (1 producer, 4-6 weeks)

- Pick a producer with moderate data volume, low consumer blast radius, technically strong team
- Run the full 8-phase flow end-to-end including a failover drill
- Capture lessons: what failed, what was confusing, what the recipe got wrong
- Update presets and recipes based on pilot findings

### Wave 2: Early adopters (3-5 producers, 2-3 months)

- Producers with similar tech stacks to the pilot
- Weekly office hours with platform team
- Track migration-completion metrics per producer (see §7)

### Wave 3: Broad rollout

- Self-serve for producers with any of the three supported stacks
- Move from weekly office hours to async channels (dedicated Slack, docs)
- Platform team focuses on incident response and fleet-level observability

### Anti-patterns

- **Big-bang rollout.** Don't migrate 30 producers in parallel. You'll spend more time firefighting than the sequential rollout would have cost.
- **Skipping the pilot.** Your recipes are opinionated but untested in your specific environment. The pilot is where your preset actually gets right.
- **Cutover without reconciliation window.** Every producer must run 2-4 weeks of dual-write reconciliation. No exceptions for "simple" pipelines.

## 6. Observability

You need metrics at three levels: per-producer-per-table, per-producer, and fleet-level.

### Per-table metrics

- Snapshot count (and growth rate — alarm if >100/day unless expected)
- Active data file count and average file size (alarm on <10MB avg — compaction lagging)
- Delete file ratio (for merge-on-read tables — alarm if delete files > 20% of data files)
- Last commit timestamp (alarm if stale > expected write cadence × 2)

### Per-producer metrics

- Migration phase (0-8)
- Dual-write reconciliation pass rate (during phase 5)
- CommitFailedException rate
- Repointing latency (primary → DR)

### Fleet-level metrics

- Total producers onboarded
- Phase distribution (how many at phase 4 dual-write, how many at phase 8 multi-region)
- Producers with stale reconciliation (>2 consecutive failures)
- Total data under management (rows, bytes, tables)

### Standard dashboards

`/iceberg-multi-region` skill generates per-producer CloudWatch dashboards. Platform team layers a fleet dashboard on top that aggregates. Use Grafana or CloudWatch cross-account dashboards if producers live in separate AWS accounts.

## 7. Common incident patterns

### 7.1 CRR lag exceeds threshold

**Symptom:** `ReplicationLatency` > 30 minutes on a producer's primary bucket.

**Diagnosis:** 
1. Check CRR rule status (it can pause on IAM/permission changes).
2. Check `BytesPendingReplication` — is it flat (stuck) or growing (backfill in progress)?
3. Check producer write rate — did volume spike?

**Remediation:** If stuck, re-apply the CRR policy. If volume-driven, increase the CRR throughput quota (AWS Support ticket). If sustained, the DR bucket may be approaching request-rate limits — partition prefix sharding.

### 7.2 Repointing fails with "manifest not found"

**Symptom:** Repointing Lambda logs `NoSuchKey` on a manifest Avro.

**Diagnosis:** CRR hasn't replicated that object yet. The metadata.json references it, but the object isn't in DR yet.

**Remediation:** Retry with backoff. If persistent, check CRR replication status for that specific object (`aws s3api head-object --bucket <dr-bucket> --key <path>`). If replication is failing for that object specifically, check object size (>5GB multipart uploads have different replication semantics) and object-level ACL.

### 7.3 Producer's Iceberg commits fail after Glue catalog migration

**Symptom:** `ConcurrentModificationException` or `CommitFailedException` on every commit after a Glue schema change.

**Diagnosis:** Glue table version was updated externally (possibly by Lake Formation, a crawler, or another job). Iceberg sees its cached metadata pointer as stale.

**Remediation:** Disable Glue crawlers on Iceberg-managed databases — the crawler rewrites the table's `TableType` and breaks Iceberg's metadata pointer. Publish this as a platform-wide policy.

### 7.4 Cross-region read accidentally enabled

**Symptom:** A producer's job in us-west-2 is reading from `s3://producer-bucket-us-east-1/`.

**Diagnosis:** Someone's pipeline config was manually edited to point at the primary bucket, bypassing the DR-region Glue Catalog.

**Remediation:** Enforce with SCP (Service Control Policy) at the account level: deny `s3:GetObject` across regions. Audit periodically via CloudTrail — look for cross-region S3 API calls from producer roles.

### 7.5 Snapshot explosion

**Symptom:** `.files` metadata table shows 50,000+ snapshots on a 3-month-old table.

**Diagnosis:** Producer is writing very frequently (streaming with 1-second micro-batches, or per-record Lambda invocations), and snapshot expiration is scheduled weekly.

**Remediation:**
- Short-term: run `expire_snapshots` on demand, retain last 100.
- Long-term: either batch writes at the producer (N records or T seconds), or run snapshot expiration daily instead of weekly. Lambda producers in particular need daily expiration.

## 8. Governance checklist

Before onboarding any producer, verify:

- [ ] Producer has an org preset pointer in their profile
- [ ] Producer's IAM roles scoped to their own bucket + Glue database (not cross-producer)
- [ ] Producer's KMS access is scoped (no `kms:*` on the org-wide key)
- [ ] Producer's Lake Formation grants match the org tag schema
- [ ] Producer's multi-region region pair matches the org's allowed list (some orgs restrict to `us-east-1`/`us-west-2` only, others allow `eu-*`)
- [ ] Producer's CloudWatch alarms deploy to the central monitoring account
- [ ] Producer has a documented on-call rotation for their data pipeline
- [ ] Producer has run a failover drill end-to-end in a non-prod environment

## 9. Upgrade strategy

When you update the platform repo (new Iceberg version, new preset field, new skill):

1. Test in platform staging environment against a mock producer profile.
2. Pilot the upgrade with 1-2 volunteer producers.
3. Announce to all producers with a 2-week window to adopt.
4. After window, any producer still on old version gets a warning; 6 weeks out, errors.

Mark incompatible preset changes as a major version bump (e.g. `acme-corp-regulated` v1 → v2) and keep v1 available during transition.

## 10. When to escalate to an Iceberg committer

Most incidents resolve in the recipes and skills. Escalate to an upstream Iceberg maintainer when:

- Metadata file corruption that survives repointing
- Snapshot rollback fails with inconsistent table state
- Compaction produces incorrect data (rare but reported)
- Behavior differs between Spark, PyIceberg, and Java APIs on the same table

File a GitHub issue in `apache/iceberg` with reproduction steps. Do not attempt hand-edits to `metadata.json` under any circumstances.
