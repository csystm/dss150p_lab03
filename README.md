# DSS150P Lab 3 — Modular Data Pipeline

A reproducible, modular, rerun-safe, partitioned, and orchestrated data pipeline
for an e-commerce analytics platform. Built for DSS150P Module 2: Pipeline
Construction, Storage, and Orchestration.

## What it does

Raw customer/product/order exports → staging → curated sales order lines → PostgreSQL,
with quarantine for invalid and orphan records, benchmarked across CSV/JSONL/Parquet/PostgreSQL,
partitioned Parquet by year/month, and scheduled through Apache Airflow.

## Architecture at a glance

```
data/source/        ──extract──>  data/raw/run_id=<id>/
                                    │
                              ──staging──>  data/staging/run_id=<id>/  +  data/quarantine/
                                    │
                              ──curated──>  data/curated/sales_order_lines.parquet
                                    │
                              ──load───>  PostgreSQL curated.sales_order_lines
                                          PostgreSQL audit.pipeline_runs
```

The same pipeline is driven three ways:

- **Manually** via `python -m src.cli <command>`
- **In a container** via `docker compose run --rm pipeline python -m src.cli <command>`
- **On a schedule** via the Airflow DAG `dss150p_sales_pipeline`

## Quickstart

### 1. Environment

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env
# edit .env and set POSTGRES_PASSWORD
python -m src.cli validate-env
```

### 2. Database

```bash
docker compose up -d postgres
docker compose ps                    # wait for "healthy"
```

The `sql/init/` scripts create the `dss150p` and `airflow` databases and the
`staging`, `curated`, `audit` schemas on first boot.

### 3. Run the pipeline

```bash
python -m src.cli run-all            # extract -> transform -> load -> validate
python -m src.cli validate           # re-check the curated layer
```

Repeated `load` on unchanged data reports `upserted_rows=0` — the row-level
`record_hash` guard prevents unnecessary updates.

### 4. Benchmarks and partitioning

```bash
python -m src.cli benchmark --repeats 5
python -m src.cli partition
python -m src.cli load-partition --year 2026 --month 1
```

Results land in `data/benchmarks/benchmark_results.csv`.

### 5. Airflow

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d \
  airflow-webserver airflow-scheduler
```

Airflow UI: `http://localhost:8080` (training credentials `admin` / `admin`).

The DAG `dss150p_sales_pipeline`:

- Schedule: `0 2 * * *` (daily 02:00 UTC)
- Parameters: `run_mode` (`full` / `partition`), `year`, `month`
- Dependency chain: `extract >> transform >> load >> validate`
- Retries: 2 with 1-minute delay; per-task timeout 10 minutes
- `on_failure_callback` prints task_id, run_id, try_number, error
- `catchup=False` (backfill is an explicit, manual decision)

### 6. Integrated acceptance test

From a clean state (empty DB tables, no `data/raw`…`data/partitioned`), run:

```bash
bash scripts/run_acceptance.sh
```

The full log is written to `docs/evidence/integrated_acceptance.txt` with
stage banners, timings, and exit codes.

## CLI reference

| Command | Purpose |
|---|---|
| `validate-env` | Print resolved config; confirm `.env` + `settings.yml` load |
| `extract` | Copy source files into `data/raw/run_id=<id>/` with SHA-256 manifest |
| `transform` | Raw → staging → curated; writes quarantine for rejected rows |
| `load` | UPSERT the curated Parquet into `curated.sales_order_lines` |
| `validate` | Assert curated-layer rules; non-zero exit if any fail |
| `benchmark --repeats N` | Time CSV/JSONL/Parquet/PostgreSQL; median of N runs |
| `partition` | Write Hive-style partitioned Parquet by `order_year`/`order_month` |
| `load-partition --year Y --month M` | UPSERT one partition; record in `audit.partition_loads` |
| `run-all` | Full pipeline: extract → transform → load → validate |

`PIPELINE_RUN_ID` overrides the auto-generated run id when set; the Airflow DAG
uses this to keep one run id across all four tasks.

## Project layout

```
config/settings.yml        Non-secret pipeline defaults
.env.example               Environment-variable template (secrets go in .env)
src/cli.py                 Thin dispatcher — no business logic
src/config.py              Loads settings.yml + .env into one config object
src/common/audit.py        Run id, UTC timestamps, deterministic record_hash
src/common/errors.py       PipelineStageError with stage + run_id context
src/extract/               Raw snapshot with manifest
src/transform/staging.py   Typing, dedup, per-dataset quality rules
src/transform/curated.py   Joins, monetary measures, orphan quarantine
src/load/postgres.py       Rerun-safe UPSERT and partition load
src/validate/quality.py    Curated-layer assertions
src/benchmark/storage.py   Four-format benchmark and partitioned Parquet writer
dags/dss150p_pipeline.py   Airflow DAG (coordination only)
sql/init/                  Database and schema bootstrap
scripts/run_acceptance.sh  End-to-end acceptance test runner
docs/answers.md            Write-up with evidence links
docs/evidence/             Raw command outputs and screenshots per goal
```

## Design principles

- **Raw is immutable.** Every run copies source files byte-for-byte into a fresh
  `data/raw/run_id=<id>/` and records each file's SHA-256.
- **Invalid records are never silently dropped.** They go to `data/quarantine/`
  with a reason. System failures raise `PipelineStageError` and exit non-zero.
- **Loads are rerun-safe.** `record_hash` over business content + UPSERT with
  `WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash` means an unchanged
  rerun is a no-op.
- **Configuration is separated from code.** Secrets live in `.env`; non-secrets
  in `config/settings.yml`. No password appears in any tracked file.
- **Airflow orchestrates, it does not transform.** The DAG invokes `src.cli`;
  all transformation logic lives in `src/`.

## Evidence

Per-goal evidence (command outputs, screenshots, query results) lives in
`docs/evidence/goal1/` … `docs/evidence/goal4/`. The technical write-up is in
`docs/answers.md`.

## Known limitations

- Source files are current-state exports, not historical snapshots, so a
  backfill relies on the source still containing the target month.
- Benchmarks are single-machine measurements; absolute numbers are not portable,
  but relative rankings reflect format design.
- Airflow runs on the local Docker host; the scheduler only fires while the
  containers are up. `catchup=False` avoids a burst of stale runs after downtime.

## AI use disclosure

Generative AI assistance was used during the development of this project for debugging errors and explaining design rationale. All generated content was reviewed, executed, and verified against the supplied source data and lab requirements. All terminal evidence in `docs/evidence/` was produced by running the committed code on this machine; no results were fabricated. The lab's engineering constraints (source files never modified, no credentials in tracked files, rerun-safe UPSERTs, invalid and orphan records quarantined rather than dropped, business logic outside the DAG) were respected throughout. The final design decisions and their justifications remain the author's responsibility.