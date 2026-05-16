---
description: "One-time onboarding that captures a producer's conventions (region, bucket, database, tech stack, orchestrator, multi-region posture) and writes profiles/<producer-name>.yaml. Run this first, before any other iceberg-* skill. Every other skill reads from the profile so you answer these questions once."
---

# Iceberg Profile Setup

## Goal
Capture a producer's conventions in a single YAML file at `profiles/<producer-name>.yaml` so every other Iceshelf skill reads from it instead of asking the producer the same questions repeatedly.

## When to run
- **First** time a producer uses Iceshelf (before `/iceberg-onboard`)
- When a producer's conventions change (region pair, tech stack, orchestrator)
- When upgrading to a new org preset version

## Step 1: Check for existing profile

Look in `profiles/` for a file matching the producer name. If found, confirm with the producer whether to edit, regenerate, or keep as-is.

```bash
ls profiles/ | grep <producer-name>
```

If `profiles/presets/` doesn't exist yet, create it.

## Step 2: Gather answers

Ask the questions in this order. Do not proceed to the next question until the current one has a valid answer. Validate each against `profiles/schema.md`.

### Required

1. **Producer name** (lowercase, dash-separated, no spaces). Must match eventual filename. Example: `datamesh-producer-brok`.

2. **Which org preset do you extend?** Offer the two built-in presets first, then ask if their org has a custom preset in `profiles/presets/`:
   - `aws-glue-datamesh-strict` — no cross-region S3, no MRAPs, per-region catalogs (regulated industries)
   - `aws-glue-central-lake` — shared bucket, cross-region S3 allowed, single Glue catalog
   - `<org-custom>` — if their org has one (ask for its filename)

3. **Primary region?** (e.g. `us-east-1`). Validate against known AWS region codes.

4. **Will you enable multi-region?** (yes/no/later)
   - If yes: ask for **DR region** and verify it differs from primary.
   - If later: set `multi_region.enabled: false`; they'll update at Phase 7.

5. **Warehouse bucket (primary region).** Bucket name only, no `s3://` prefix, no path. If they haven't created it yet, note it for the pre-flight checklist.

6. **Warehouse bucket (DR region)** — only if multi-region enabled.

7. **Glue database name.** Typically matches `{producer_name}_{stage}` convention (e.g. `brok_confirmed`) — suggest this if their preset has `naming_conventions.database_pattern`.

8. **Tech stack?** One of:
   - `glue_pyspark`
   - `emr_pyspark`
   - `ecs_python`
   - `lambda_python`
   - `ecs_java`
   - `lambda_java`

9. **Orchestrator?** One of: `step_functions`, `mwaa`, `airflow`, `glue_workflows`, `dagster`. Default from preset if not set.

### Optional (only ask if the producer has non-default needs)

10. **Are you migrating from existing Parquet, or starting fresh?** Records intent — doesn't go in the YAML, but informs which recipe phases to highlight next.

11. **Any custom partition defaults?** Ask only if they want `copy-on-write` (read-heavy) vs default `merge-on-read`, or non-standard file size / compression.

12. **KMS key alias?** Only if they have a non-default KMS posture beyond the preset.

## Step 3: Build the profile

Write `profiles/<producer-name>.yaml`. Keep it minimal — only fields that differ from the preset should appear. Example output:

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

For a producer who's not ready for multi-region:

```yaml
producer_name: datamesh-producer-nova
extends: aws-glue-datamesh-strict

primary_region: eu-west-1
warehouse_bucket_primary: nova-confirmed-eu-west-1
database: nova_confirmed
tech_stack: ecs_python
orchestrator: mwaa

multi_region:
  enabled: false
```

## Step 4: Validate

Invoke the `iceberg-validator` agent (if available) or inline-validate:

- File name matches `producer_name`
- All required fields present (see `profiles/schema.md`)
- `primary_region` is a valid AWS region code
- `dr_region != primary_region` if multi-region enabled
- `tech_stack` is one of the 6 supported values
- `extends` file exists in `profiles/presets/`

Report any validation failures back to the producer and let them correct before writing.

## Step 5: Point them to the next step

After the profile is written, direct the producer to:

1. Their tech-stack recipe:
   - `glue_pyspark` or `emr_pyspark` → [docs/examples/pyspark-producer-recipe.md](../../docs/examples/pyspark-producer-recipe.md)
   - `ecs_python` or `lambda_python` → [docs/examples/pyiceberg-producer-recipe.md](../../docs/examples/pyiceberg-producer-recipe.md)
   - `ecs_java` or `lambda_java` → [docs/examples/java-producer-recipe.md](../../docs/examples/java-producer-recipe.md)

2. Phase 1: run `/iceberg-onboard` to begin assessment.

Confirm the profile path they should reference: `profiles/<producer-name>.yaml`.

## Step 6: Commit the profile

Suggest the producer commit the profile YAML to their repo. Example message:

```
Add Iceshelf profile for <producer-name>

Preset: aws-glue-datamesh-strict
Region pair: us-east-1 / us-west-2
Stack: glue_pyspark
Orchestrator: step_functions
```

## Notes

- **Do not write secrets into the profile.** KMS key *alias* is fine; key ARNs with sensitive account IDs should be referenced via SSM Parameter Store or env var, not inlined.
- **Do not hardcode tags in the profile** that belong to a producer-team-specific value (Owner, CostCenter). Those come from the preset or the producer's IaC.
- **Do not invent fields** not in `profiles/schema.md`. If the producer has a need not in the schema, flag it as a platform-team change request rather than adding a one-off field.
