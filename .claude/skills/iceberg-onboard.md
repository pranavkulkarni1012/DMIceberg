---
description: "Onboard a producer onto Iceberg end-to-end. Use for complete onboarding of a new producer (greenfield) or migrating an existing Parquet producer to Iceberg. Coordinates assessment, code generation, validation, and optionally multi-region setup into a single deliverable."
---

# Iceberg Producer Onboarding

## Goal
Gather the producer's requirements, then delegate to the `iceberg-orchestrator` agent for end-to-end onboarding. This skill is the single entry point for any producer -- new or existing -- who needs a complete Iceberg setup.

## Step 1: Gather Requirements

Ask the producer for the following (skip what's already provided):

1. **Producer type**: New (no existing pipeline) or Existing (has a Parquet pipeline to migrate)?

2. **For existing producers**:
   - Where is the codebase? (path, repo, or describe the pipeline)
   - Which tables need migration?
   - Can writes be paused during migration?

3. **Tech stack** (if known -- the orchestrator can auto-detect for existing producers):
   - Glue PySpark / EMR PySpark / ECS Python / Lambda Python / ECS Java / Lambda Java

4. **Table details**:
   - Database name
   - Table name
   - S3 bucket and warehouse path (convention: `s3://<bucket>/warehouse/`)
   - AWS region

5. **Data characteristics**:
   - Schema (column names and types, or "infer from existing code/data")
   - Partitioning strategy (by date, by category, or "recommend")
   - Write pattern: append-only, upsert, or full refresh
   - Merge/primary key columns (if upsert)
   - Estimated volume per batch/day

6. **Data source** (for new producers):
   - Where does data come from? (S3, JDBC, API, Kafka, Kinesis, SQS)

7. **Multi-region**: Is multi-region resilience needed?
   - If yes: target region(s) and target S3 bucket(s)
   - Sync frequency (after every commit, hourly, daily)

8. **Scheduling**: How often does the pipeline run?

9. **Monitoring**: CloudWatch, SNS alerts, custom?

## Step 2: Delegate to Orchestrator

Once requirements are gathered, spawn the `iceberg-orchestrator` agent with all collected information. Use `subagent_type: "iceberg-orchestrator"` when calling the Agent tool.

Pass the orchestrator a prompt that includes:
- Producer type (new or existing)
- All gathered requirements from Step 1
- Codebase path (for existing producers)
- Any constraints or preferences the user mentioned

The orchestrator will:
1. **Assess** the producer (codebase analysis for existing, requirement validation for new)
2. **Plan** which artifacts to generate (presents plan for user approval)
3. **Generate** all code via specialized subagents
4. **Validate** everything via the validator subagent
5. **Deliver** a cohesive, deployment-ready package

## Step 3: Present Results

The orchestrator returns a complete onboarding package. Present it to the user with:
- Summary of what was generated
- Deployment sequence (ordered steps)
- All generated code artifacts
- Validation results
- Post-deployment checklist
- Rollback plan (for existing producers)

## When NOT to Use This Skill

Direct the user to individual skills instead when they need:
- Just a single table created --> `/iceberg-ddl`
- Just a MERGE INTO statement --> `/iceberg-data`
- Just a health check --> `/iceberg-info`
- Just maintenance setup --> `/iceberg-maintenance`
- Just multi-region added to an already-working Iceberg table --> `/iceberg-multi-region`

This skill is for **complete onboarding**, not targeted single operations.
