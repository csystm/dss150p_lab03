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

### Task B — Modular structure
`src/cli.py` is a thin dispatcher. It doesn't do real work itself — it just looks at the command and calls the right module. Extraction lives in `src/extract/`, staging in `src/transform/staging.py`, curated in `src/transform/curated.py`, loading in `src/load/`, validation in `src/validate/`, benchmarking in `src/benchmark/`. Each module has one job. This makes the code easier to test, review, and reuse from Airflow later.

### Task C — Configuration and secrets
Two different kinds of settings, kept separate:

- **Non-secret defaults** (file paths, allowed statuses, quantity limits) live in `config/settings.yml`. Committed.
- **Secrets and machine-specific values** (like the database password) live in `.env`. **Not** committed. `.env.example` shows what's needed without exposing the actual value.

`src/config.py` is the only file that reads both. No password appears in any Python, YAML, SQL, or DAG file that goes into Git. The compose files also refuse to start if `POSTGRES_PASSWORD` is missing, so it can't accidentally run with a blank password. Verified with `git grep "change_me"` — no hits outside `.env.example`.

### Task D — Docker and Compose
`Dockerfile` builds a small Python image with our dependencies. `docker-compose.yml` runs two services:

- `postgres` — the database, with our SQL init scripts mounted so the schemas and tables are created the first time it starts.
- `pipeline` — our code, using the same CLI commands as the local runs.

Inside the Docker network, PostgreSQL is reachable at hostname `postgres`, not `localhost`, so the pipeline service overrides `POSTGRES_HOST=postgres` and `POSTGRES_PORT=5432`. That way the same code works whether we run it locally or in a container — no hard-coded hostnames.

Verified: `docker compose ps` shows Postgres healthy; `docker compose run --rm pipeline python -m src.cli validate-env` prints the same output as the local run; `\dn` and `\dt` show the three schemas and three tables we expect.

### Task E — Git workflow
Work was done on a branch (`goal1-reproducible-environment`) and merged to `main`. Meaningful commit messages describe each change. `.env` is ignored by Git and confirmed with `git check-ignore .env`.

## Goal 2 — ETL/ELT Pipeline

### Evidence
| Artifact | Path |
|---|---|
| Raw extraction run output | [extract.txt](evidence/goal2/extract.txt) |
| Raw directory tree | [raw_tree.txt](evidence/goal2/raw_tree.txt) |
| Raw manifest (sizes + SHA-256) | [raw_manifest.json](evidence/goal2/raw_manifest.json) |
| Source unchanged after extract | [source_unchanged.txt](evidence/goal2/source_unchanged.txt) |
| Staging valid/quarantine counts | [staging_counts.txt](evidence/goal2/staging_counts.txt) |
| Curated counts and sample rows | [curated_counts.txt](evidence/goal2/curated_counts.txt) |
| Healthy `run-all` | [run_all_full.txt](evidence/goal2/run_all_full.txt) |
| Deliberate stage failure | [stage_error.txt](evidence/goal2/stage_error.txt) |
| First load | [load_first.txt](evidence/goal2/load_first.txt) |
| Second load (rerun-safe) | [load_second.txt](evidence/goal2/load_second.txt) |
| DB duplicate-key check | [load_rerun_safe.txt](evidence/goal2/load_rerun_safe.txt) |
| Load from inside container | [load_from_container.txt](evidence/goal2/load_from_container.txt) |
| Validation pass | [validate_ok.txt](evidence/goal2/validate_ok.txt) |
| Validation detects bad rows | [validate_detects_bad_rows.txt](evidence/goal2/validate_detects_bad_rows.txt) |
| Layer-by-layer row counts | [layer_counts.txt](evidence/goal2/layer_counts.txt) |

### Task A — Raw extraction
Each `extract` run writes a new folder `data/raw/run_id=<id>/` and copies the three source files byte-for-byte. Nothing is modified in `data/source/`. A `manifest.json` inside the run folder records each copied file's size and SHA-256, so anyone can confirm the snapshot matches the source. Source files are never overwritten — every run gets its own snapshot.

### Task B — Staging
Staging cleans and types the source data, then splits it into "valid" and "quarantine" based on explicit rules. Every dataset is deduplicated by its business key (`customer_id`, `product_id`, `order_id`), keeping the version with the latest `updated_at`. Counts from the latest run:

| Dataset | Valid | Quarantined | Reason |
|---|---|---|---|
| customers | 2996 | 4 | `missing_email` |
| products | 599 | 1 | `invalid_price` |
| orders | 49998 | 2 | `invalid_quantity`, `invalid_status` |

Valid rows are written as Parquet under `data/staging/run_id=<id>/`. Invalid rows go to `data/quarantine/run_id=<id>/quarantine.parquet` with the original record, the reason, and the run id attached.

### Task C — Curated
Valid orders are joined to valid customers and valid products. Orders that reference a customer or product we don't have are **not dropped** — they go to quarantine with `orphan_customer`, `orphan_product`, or `orphan_customer_and_product`. Counts:

| Layer | Count |
|---|---|
| Orders in staging (valid) | 49998 |
| Orphan orders quarantined | 164 |
| **Curated rows** | **49834** |

Monetary columns are computed from the source fields:

- `gross_amount = quantity * unit_price`
- `discount_amount = gross_amount * discount_pct`
- `net_amount = gross_amount - discount_amount`

All three rounded to 2 decimals. `record_hash` is a SHA-256 over the business columns so the same input always produces the same hash.

### Task D — Errors vs data quality
Two different failure types, handled differently:

- **Data-quality problems** (missing email, invalid price, bad status, orphan reference) are *expected*. They don't crash the pipeline — they're written to quarantine with a reason. That preserves the record for diagnosis and reprocessing.
- **System problems** (missing raw folder, DB unreachable, schema mismatch) are *unexpected*. They raise `PipelineStageError`, which carries the stage name and run id, and the CLI exits non-zero so a scheduler can react.

Evidence of the deliberate stage failure: [stage_error.txt](evidence/goal2/stage_error.txt) — shows the exact stage, run id, exit code.

### Task E — Rerun-safe loading
`curated.sales_order_lines` is written with `INSERT ... ON CONFLICT (order_id) DO UPDATE`. The `DO UPDATE` is guarded by `WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash`, so a row whose content hasn't changed is left untouched.

Proof:

- First load: [load_first.txt](evidence/goal2/load_first.txt) → `upserted_rows=49834`
- Second load (same data): [load_second.txt](evidence/goal2/load_second.txt) → `upserted_rows=0`
- DB check: [load_rerun_safe.txt](evidence/goal2/load_rerun_safe.txt) → `total = distinct_orders = 49834`

The second load reporting `0` is the key number: the pipeline recognized the data was identical and skipped every row instead of rewriting it.

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