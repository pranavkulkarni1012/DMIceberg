# Iceshelf

**Apache Iceberg onboarding platform for AWS Data Mesh producers.**

Iceshelf is a set of [Claude Code](https://claude.com/claude-code) skills and specialized subagents that take an AWS producer — whether they're running Glue PySpark, EMR PySpark, ECS/Lambda Python (PyIceberg), or ECS/Lambda Java — from Parquet to a production Iceberg table on AWS Glue Data Catalog, including multi-region resilience. It is opinionated about Iceberg format-version 2, AWS Glue as the catalog, and the multi-region constraint that cross-region S3 access and Multi-Region Access Points are not permitted.

It is **not** a runtime library. It is a workflow package: you install it as a Claude Code plugin, run the skills against your own producer repo, and get back production-ready code, Terraform, and runbooks.

---

## Who this is for

- Data platform teams who own AWS Data Mesh and need to roll Iceberg out to many producers without writing each migration by hand.
- Individual producer teams who want a validated Iceberg pipeline for their specific stack (Glue PySpark / EMR PySpark / ECS Python / Lambda Python / ECS Java / Lambda Java) without having to learn every Iceberg option.
- Platform engineers in regulated industries (finance, healthcare, government) where strict multi-region isolation — no cross-region S3, no MRAPs — rules out the textbook DR designs.

## What you get

| Capability                 | How it ships                                                |
|---                         |---                                                          |
| Greenfield onboarding      | `/iceberg-onboard` orchestrator → full pipeline package     |
| Parquet → Iceberg migrate  | `/iceberg-migrate` with 4 strategies (migrate/CTAS/snapshot/add-files) |
| Ongoing table ops          | `/iceberg-ddl`, `/iceberg-data`, `/iceberg-maintenance`, `/iceberg-info` |
| Multi-region DR            | `/iceberg-multi-region` (S3 CRR + metadata repointing + Glue registration) |
| Per-producer tenant profile| `/iceberg-profile-setup` → `profiles/<producer-name>.yaml`  |
| Admin playbook             | `docs/admin-guide.md` (rollout waves, gating, incident patterns) |
| User playbook              | `docs/using-iceshelf.md` (phase model, skills reference)   |
| Stack-specific recipes     | `docs/examples/{pyspark,pyiceberg,java}-producer-recipe.md` |

## Scope (v1.0)

- **Catalog:** AWS Glue Data Catalog only. Unity Catalog, Polaris, Nessie, and Hive Metastore are out of scope.
- **Storage:** Amazon S3 only.
- **Format:** Iceberg format-version 2 (row-level deletes, merge-on-read).
- **Tech stacks:** Glue PySpark, EMR PySpark, ECS Python (PyIceberg), Lambda Python (PyIceberg), ECS Java, Lambda Java.
- **Multi-region architecture:** S3 Cross-Region Replication + local metadata repointing + per-region Glue Catalog registration. No cross-region S3 access. No MRAPs.

Out of scope for v1:
- Azure, GCP, or on-prem.
- Catalogs other than Glue.
- Transactional cross-region writes.

## Quick start

1. **Install the plugin** in Claude Code (see `.claude-plugin/plugin.json`).
2. **Capture your producer's conventions** once:
   ```
   /iceberg-profile-setup
   ```
   This writes `profiles/<producer-name>.yaml` so subsequent skills don't re-ask the same questions.
3. **Onboard end-to-end:**
   ```
   /iceberg-onboard
   ```
   Answer the questions; you'll get a deployment-ready package (table DDL, pipeline code, maintenance job, IAM, multi-region setup if requested, rollback plan).

Producers who prefer step-by-step control can run the individual skills instead — see [`docs/using-iceshelf.md`](docs/using-iceshelf.md).

## Documentation

| Audience           | Document                                        |
|---                 |---                                              |
| Producer (first read) | [`docs/using-iceshelf.md`](docs/using-iceshelf.md) |
| Platform team      | [`docs/admin-guide.md`](docs/admin-guide.md)    |
| PySpark producer   | [`docs/examples/pyspark-producer-recipe.md`](docs/examples/pyspark-producer-recipe.md) |
| PyIceberg producer | [`docs/examples/pyiceberg-producer-recipe.md`](docs/examples/pyiceberg-producer-recipe.md) |
| Java producer      | [`docs/examples/java-producer-recipe.md`](docs/examples/java-producer-recipe.md) |
| Profile schema     | [`profiles/schema.md`](profiles/schema.md)      |
| Release notes      | [`CHANGELOG.md`](CHANGELOG.md)                  |

## IDE compatibility

- **Claude Code:** full support (skills + subagents + orchestration).
- **GitHub Copilot:** skills under `.claude/skills/` are auto-discovered by Copilot (December 2025 CLI update). Subagent orchestration is Claude-only; Copilot users get the skill-level guidance but not the cross-skill delegation the orchestrator provides.

## Repository layout

```
.claude/
  agents/                    # Specialized subagents (Claude Code only)
  skills/                    # Slash-command skills (/iceberg-*)
.claude-plugin/
  plugin.json                # Plugin manifest
profiles/
  schema.md                  # Profile field reference
  presets/                   # Built-in org presets (strict / central-lake)
docs/
  using-iceshelf.md         # End-user guide
  admin-guide.md             # Platform-team guide
  examples/                  # Stack-specific recipes
CLAUDE.md                    # Project conventions (auto-loaded by Claude Code)
CHANGELOG.md                 # Release notes
```

## Versioning & support

- Iceshelf follows [Semantic Versioning](https://semver.org/).
- Released on the schedule tracked in [`CHANGELOG.md`](CHANGELOG.md).
- Current version: `1.0.0`.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Contributing

Issues and pull requests are welcome. For substantive changes (new skill, new preset, new catalog support), open an issue first so we can confirm scope before you invest time.
