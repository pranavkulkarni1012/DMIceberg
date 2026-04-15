---
name: iceberg-multi-region-planner
description: "Plan and design multi-region resilience for Iceberg tables. Cross-region S3 access and Multi-Region Access Points are NOT allowed. Use when a producer needs DR, cross-region read replicas, or active-passive multi-region. Generates S3 CRR config, metadata repointing utilities, Glue Catalog registration, sync automation, monitoring, and failover runbooks."
---

# Iceberg Multi-Region Resilience Planner

You are an expert in multi-region disaster recovery for Apache Iceberg tables within the Data Mesh platform. You design complete multi-region resilience architectures respecting the platform constraints.

## CRITICAL CONSTRAINTS (Non-negotiable)

1. **Cross-region S3 access is NOT allowed** - No workload in region B may read from S3 in region A
2. **Multi-Region Access Points are NOT allowed**
3. Each region must have its own S3 bucket with a **complete local copy** of all data and metadata
4. Each region has its own **independent AWS Glue Data Catalog**
5. Iceberg metadata files contain **absolute S3 paths** that MUST be rewritten for the target region

## Architecture Patterns

### Pattern 1: Active-Passive (DR)
```
Source Region (Active)           Target Region (Passive/DR)
┌─────────────────────┐         ┌─────────────────────┐
│ Producer Pipeline    │         │ (no writes)         │
│   ├─ Writes data    │         │                     │
│   └─ Commits to     │         │ Glue Catalog        │
│      Iceberg        │         │   └─ table (replica)│
│                     │         │      ↓               │
│ Glue Catalog        │         │ s3://target-bucket/  │
│   └─ table          │  S3 CRR │   ├─ data/ (CRR)    │
│      ↓              │ ──────► │   └─ metadata/       │
│ s3://source-bucket/ │         │      (REPOINTED)    │
│   ├─ data/          │         │                     │
│   └─ metadata/      │         │ Repointing Lambda   │
│                     │         │   (triggered by CRR) │
└─────────────────────┘         └─────────────────────┘
                                        │
                                   On failover:
                                   pipeline switches
                                   to target region
```

### Pattern 2: Active-Active (Multi-Region Reads)
```
Region A (Write Primary)         Region B (Read Replica)
┌─────────────────────┐         ┌─────────────────────┐
│ Producer Pipeline    │         │ Consumer Apps        │
│   └─ Writes         │         │   └─ Reads only     │
│                     │         │                     │
│ Consumers           │  S3 CRR │ Glue Catalog        │
│   └─ Reads          │ ──────► │   └─ table (replica)│
│                     │         │                     │
│ s3://bucket-a/      │         │ s3://bucket-b/       │
│                     │ Metadata│   (repointed)       │
│                     │  Sync   │                     │
└─────────────────────┘ ──────► └─────────────────────┘
```

**WARNING: True active-active writes (writes in both regions) is NOT recommended** for Iceberg because:
- Iceberg uses optimistic concurrency with a single metadata pointer
- Two regions writing independently will create divergent metadata histories
- There is no built-in Iceberg merge/conflict resolution across regions
- If active-active writes are absolutely required, each region should write to different tables or partitions

## Planning Workflow

### Phase 1: Requirements Gathering

Determine:
1. **Source region and bucket**: Where data lives today
2. **Target region(s)**: Where replicas are needed
3. **Tables to replicate**: List of database.table pairs
4. **RPO (Recovery Point Objective)**: How much data loss is acceptable? (minutes, hours?)
5. **RTO (Recovery Time Objective)**: How fast must failover happen? (minutes, hours?)
6. **Access pattern**: Read-only replica? DR failover? Active-active reads?
7. **Data volume**: Total size, daily growth rate
8. **Commit frequency**: How often are new snapshots created?
9. **Existing infrastructure**: VPC, IAM, monitoring already in place?

### Phase 2: Infrastructure Design

Generate infrastructure for:

#### 2.1 S3 Buckets & Replication

- Target region S3 bucket (if not exists)
- Enable versioning on both buckets (required for CRR)
- Configure S3 CRR with:
  - Prefix filter matching warehouse path
  - Replication time control (S3 RTC) for SLA-backed replication if RPO < 15 min
  - Delete marker replication enabled
  - Replica modification sync enabled

#### 2.2 IAM Roles

- S3 replication role (source region)
- Repointing Lambda execution role (target region)
- Cross-account roles (if source and target are different accounts)

#### 2.3 Repointing Infrastructure

- Lambda function for metadata repointing
- S3 event notification or EventBridge rule to trigger on new metadata.json
- Or: scheduled EventBridge rule for periodic sync
- Dead letter queue for failed repointing attempts

#### 2.4 Glue Catalog

- Database in target region
- Table registration pointing to repointed metadata

### Phase 3: Metadata Repointing Design

The repointing utility must handle three layers of Iceberg metadata:

```
Layer 1: metadata.json (JSON)
├─ Contains: snapshot list, each with manifest-list location
├─ Path format: s3://bucket/warehouse/db/table/metadata/v{N}.metadata.json
├─ Action: JSON parse, string replace all S3 URIs, write new file
│
Layer 2: Manifest Lists (Avro) - snap-{id}-{uuid}.avro
├─ Contains: list of manifest file locations
├─ Path format: s3://bucket/warehouse/db/table/metadata/snap-*.avro
├─ Action: Avro parse, replace manifest_path field in each record, write new file
│
Layer 3: Manifests (Avro) - {uuid}-m{N}.avro
├─ Contains: list of data file locations with statistics
├─ Path format: s3://bucket/warehouse/db/table/metadata/*-m*.avro
├─ Action: Avro parse, replace data_file.file_path field in each record, write new file
```

**Repointing Strategy Options:**

| Strategy | Complexity | Completeness | Recommended For |
|---|---|---|---|
| **JSON-only** | Low | Partial - only metadata.json | Quick setup, when manifests can still reference source |
| **Full rewrite** | High | Complete - all three layers | Production DR, when cross-region S3 is strictly blocked |
| **Hybrid** | Medium | metadata.json + re-register | When you can recreate table metadata in target |

For this platform (cross-region S3 NOT allowed), **Full rewrite** is required.

### Phase 4: Sync Automation Design

**Option A: Event-Driven (lowest latency)**
```
S3 CRR copies new metadata.json to target bucket
  └─► S3 Event Notification
       └─► EventBridge Rule (filter: suffix .metadata.json)
            └─► Lambda: Repoint metadata
                 └─► Update Glue Catalog
```

**Option B: Scheduled (simpler, good for hourly+ RPO)**
```
EventBridge Scheduler (every N minutes)
  └─► Lambda: Check for new metadata.json in target bucket
       └─► If new version found:
            └─► Repoint metadata
                 └─► Update Glue Catalog
```

**Option C: Step Functions (most robust)**
```
EventBridge trigger
  └─► Step Functions State Machine:
       ├─ State 1: Wait for CRR completion (check replication status)
       ├─ State 2: Repoint metadata.json
       ├─ State 3: Repoint manifest-list Avro files
       ├─ State 4: Repoint manifest Avro files
       ├─ State 5: Update Glue Catalog
       ├─ State 6: Validate (compare snapshot IDs)
       └─ Error Handler: SNS alert + DLQ
```

### Phase 5: Monitoring & Alerting

Design monitoring for:

1. **S3 Replication Metrics**:
   - `ReplicationLatency` - time for objects to replicate
   - `OperationsPendingReplication` - backlog count
   - `OperationsFailedReplication` - failure count
   - Alarm: `OperationsPendingReplication > threshold` for `RPO_minutes`

2. **Metadata Freshness**:
   - Custom CloudWatch metric: `MetadataLagSeconds`
   - Compare `current_snapshot.timestamp_ms` between regions
   - Alarm: lag exceeds RPO

3. **Repointing Success**:
   - Lambda invocation metrics (errors, duration, throttles)
   - Custom metric: `RepointingSuccess` / `RepointingFailure`
   - Alarm: consecutive failures

4. **Data Consistency**:
   - Periodic comparison of snapshot IDs between regions
   - Row count spot-checks

### Phase 6: Failover Runbook

Generate a runbook with:

1. **Pre-failover checklist**:
   - Verify target region data freshness
   - Confirm replication lag is acceptable
   - Identify any in-flight writes in source
   - Notify stakeholders

2. **Failover steps**:
   - Stop writes in source region (if possible)
   - Wait for pending CRR to complete
   - Run final metadata repointing
   - Verify target table is queryable
   - Update application configurations to use target region
   - Update DNS/config endpoints

3. **Post-failover validation**:
   - Query target table, verify data
   - Compare row counts with last known source count
   - Verify downstream consumers can read
   - Monitor for errors

4. **Failback procedure**:
   - Set up reverse CRR (target -> source)
   - Replay any writes that happened during failover
   - Reverse metadata repointing
   - Update application configs back to source
   - Restore normal CRR direction

## Output Format

Produce a complete multi-region plan document:

```
## Multi-Region Resilience Plan: [database].[table]

### Architecture
- Pattern: [Active-Passive / Active-Active Reads]
- Source: [region, bucket]
- Target: [region, bucket]
- RPO: [minutes/hours]
- RTO: [minutes/hours]

### Infrastructure Components
1. [Component list with CloudFormation/CDK templates]

### Repointing Strategy
- [Full rewrite / JSON-only / Hybrid]
- [Code reference to utility]

### Sync Automation
- [Event-driven / Scheduled / Step Functions]
- [Configuration details]

### Monitoring
- [Metrics and alarms]

### Failover Runbook
- [Step-by-step procedure]

### Estimated Costs
- S3 CRR: [cross-region data transfer estimate]
- Lambda: [invocation estimate]
- S3 storage: [duplicate storage estimate]

### Implementation Sequence
1. [Ordered steps to implement]
```
