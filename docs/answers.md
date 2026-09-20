# DSS150P Lab 3 — Answers & Evidence Index

## Goal 1 — Reproducible Environment
| Artifact | Path |
|---|---|
| Local Python version | [python_version.txt](evidence/goal1/python_version.txt) |
| Local installed packages | [pip_freeze.txt](evidence/goal1/pip_freeze.txt) |
| Local `validate-env` | [validate_env_local.txt](evidence/goal1/validate_env_local.txt) |
| Docker image build | [docker_build.txt](evidence/goal1/docker_build.txt) |
| Compose service status | [compose_ps.txt](evidence/goal1/compose_ps.txt) |
| Container `validate-env` | [validate_env_docker.txt](evidence/goal1/validate_env_docker.txt) |
| PostgreSQL schemas (`\dn`) | [psql_schemas.txt](evidence/goal1/psql_schemas.txt) |
| `curated.*` tables | [psql_curated_tables.txt](evidence/goal1/psql_curated_tables.txt) |
| `audit.*` tables | [psql_audit_tables.txt](evidence/goal1/psql_audit_tables.txt) |
| Secret scan (`change_me`) | [grep_change_me.txt](evidence/goal1/no_secrets.txt) |
| Git log | [git_log.txt](evidence/goal1/git_log.txt) |
| Git branch | [git_branch.txt](evidence/goal1/git_branch.txt) |

### Task A — Why not commit .venv?
Don’t commit venv to Git because it contains machine-specific compiled binaries, hardcoded absolute paths, and hundreds of MB of bloat that pollute repository history. It also provides no reproducibility benefit and risks developers using a stale committed environment instead of rebuilding from the dependency contract. The correct approach is to commit pinned requirements and the documented Python version, then let them recreate the venv.

## Goal 2 — ETL/ELT Pipeline
### Task D — Exceptions vs quarantine
(answer)

## Goal 3 — Storage & Formats
### Q1–Q5
(answer per question)

## Goal 4 — Airflow
### Backfill reasoning
(answer)

## Section 15 — Technical Questions
1. record_hash: ...
2. raw preservation: ...
3. data-quality rejection vs system exception: ...
4. Parquet vs CSV: ...
5. DAG with all logic: ...
6. retries x idempotency: ...
7. partition cardinality trade-off: ...
8. API/DB source adaptation: ...