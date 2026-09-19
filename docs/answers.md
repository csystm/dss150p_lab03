# DSS150P Lab 3 — Answers & Evidence Index

## Goal 1 — Reproducible Environment
- [Local validate-env output](evidence/goal1/validate_env_local.txt)
- [docker compose ps](evidence/goal1/compose_ps.txt)

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