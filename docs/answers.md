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

### Evidence
| Artifact | Path |
|---|---|
| Hardware / OS context | [hardware_context.txt](evidence/goal3/hardware_context.txt) |
| Benchmark run output | [benchmark_run.txt](evidence/goal3/benchmark_run.txt) |
| Benchmark results table | [benchmark_results.csv](evidence/goal3/benchmark_results.csv) |
| Materialized file sizes | [benchmark_files.txt](evidence/goal3/benchmark_files.txt) |
| Partition run output | [partition_run.txt](evidence/goal3/partition_run.txt) |
| Partition directory tree | [partition_tree.txt](evidence/goal3/partition_tree.txt) |
| Single-partition read | [partition_single_read.txt](evidence/goal3/partition_single_read.txt) |
| First partition load | [load_partition_first.txt](evidence/goal3/load_partition_first.txt) |
| Second partition load (rerun-safe) | [load_partition_second.txt](evidence/goal3/load_partition_second.txt) |
| Partition dedup check | [load_partition_dedup.txt](evidence/goal3/load_partition_dedup.txt) |
| Partition load audit | [partition_loads_audit.txt](evidence/goal3/partition_loads_audit.txt) |
| Full reload after partition test | [load_full_restore.txt](evidence/goal3/load_full_restore.txt) |

### Task A — Four storage representations
The same 49,834 curated rows were written four ways: CSV, JSON Lines, Parquet (snappy compression), and the PostgreSQL `curated.sales_order_lines` table. File sizes are actual bytes on disk; PostgreSQL's size is measured with `pg_total_relation_size`, which includes the table and its primary-key index.

### Task B — Benchmark methodology
Each format was measured on the same machine, at the same time, with the same Python process, using `time.perf_counter()`:

- **Size** — file bytes for file formats; table+index bytes for PostgreSQL.
- **Write time** — a single write of the full dataset.
- **Full read** — read the entire dataset back into pandas; 5 runs; median reported.
- **Filtered read** — retrieve only rows with `status = 'DELIVERED'`; 5 runs; median reported.
- For file formats, the filtered read **includes reading the whole file** before filtering, because that's what a user actually experiences.
- For PostgreSQL, the filter is pushed into SQL (`WHERE status = ...`) — that's what a database is for.

#### Machine context
Measurements were taken on a Lenovo laptop running Windows 11 Home, AMD Ryzen 5 7535HS (6 cores / 12 threads), 15.19 GB RAM, Micron 512 GB SSD, Docker Desktop engine 29.8.0 (linux/amd64), Python 3.12.10, pandas 2.2.3, pyarrow 17.0.0. Full details in [hardware_context.txt](evidence/goal3/hardware_context.txt). Results are specific to this machine — absolute numbers will vary on other hardware, but the *relative* ranking is expected to hold because it reflects format design, not CPU speed.

#### Results (medians of 5 runs)
| Format | Size (bytes) | Write (s) | Full read (s) | Filtered read (s) | Rows |
|---|---|---|---|---|---|
| CSV | 14,928,701 | 0.654 | 0.195 | 0.200 | 49,834 |
| JSONL | 29,305,661 | 0.485 | 0.663 | 0.747 | 49,834 |
| Parquet | 5,479,614 | 0.114 | 0.060 | 0.062 | 49,834 |
| PostgreSQL | 15,949,824 | 0.166 | 1.197 | 0.222 | 49,834 |

On this machine:

- **Parquet is smallest** — 2.7× smaller than CSV and 5.3× smaller than JSONL.
- **Parquet is fastest to write and fastest to read** of the four.
- **PostgreSQL full-read is slowest** (1.20 s) even though its file size is close to CSV. That's the cost of pulling 49,834 rows across the psycopg wire protocol into Python objects — the database itself is not slow, the round-trip is.
- **PostgreSQL filtered read (0.22 s)** is close to CSV (0.20 s) and far better than its own full read, because the `WHERE status = ...` is evaluated inside the server before the rows cross the wire.

### Task C — Partitioned Parquet
The curated dataset was written as a partitioned Parquet dataset using two keys derived from `order_timestamp`: `order_year` and `order_month`. The directory structure is Hive-style, which is the format most tools (Spark, PyArrow, DuckDB, Athena, etc.) understand natively:

```
data/partitioned/order_year=2025/order_month=1/part-0.parquet
data/partitioned/order_year=2025/order_month=2/part-0.parquet
...
data/partitioned/order_year=2026/order_month=9/part-0.parquet
```

Reading one partition — `order_year=2026, order_month=1` — returned exactly 2,504 rows, all with `order_year=2026` and `order_month=1`. The partition keys are encoded in the directory names, not stored inside each leaf file; PyArrow's dataset API reconstructs them on read.

**Why partitioning helps:** a query that filters on the partition keys only opens the matching leaf folders. Everything else is skipped without any file I/O. On a multi-TB dataset this turns a full-table scan into a handful of file reads. The bigger the dataset, the more this matters.

### Task D — Selected-partition load
`python -m src.cli load-partition --year 2026 --month 1` loaded only the 2026-01 partition into `curated.sales_order_lines` using the same UPSERT semantics as the full load.

**Note about the "0 rows" first run:** the initial partition load reported `rows=0` because the same data had already been loaded by Goal 2's full `run-all`. The `record_hash` guard correctly recognized identical rows and skipped every update. To demonstrate the partition load from a clean state, `curated.sales_order_lines` was truncated, then the 2026-01 partition was loaded:

- First partition load after truncate: `rows=2504` (inserted).
- Second partition load (rerun, unchanged data): `rows=0` (skipped).
- DB check after both loads: `total = distinct_orders = 2504`, and `min_ts` / `max_ts` both fell inside 2026-01 — proving only that partition was present.
- The full dataset was then reloaded with `python -m src.cli load` to restore the table to 49,834 rows for Goal 4.

The 2026-01 load is recorded in `audit.partition_loads` with `partition_key = '2026-01'`.

### 9.5 Analysis questions

**1. Which file format was smallest, and why?**
Parquet (5.48 MB). Two reasons: it's columnar (values of the same type are stored together, which compresses much better than row-oriented formats), and it uses snappy compression. JSONL was the largest (29.31 MB) because every row repeats the same field names — around 100 bytes of key names per row × 49,834 rows.

**2. Which representation was fastest for a full read? Does that imply it's best for every workload?**
Parquet (0.060 s median). That doesn't mean Parquet is universally best. CSV is easier for humans to inspect and is more widely accepted by downstream tools. JSONL streams naturally (one record per line) and is better for append-only logs. PostgreSQL gives ACID transactions, concurrency, indexes, and multi-user access. Each format is best for a specific kind of workload — the fastest read on this dataset says nothing about writes at scale, concurrency, or tooling compatibility.

**3. How did filtered retrieval differ between Parquet and PostgreSQL? What PostgreSQL design could change the result?**
Parquet's filtered read (0.062 s) still reads the whole file, because pandas loads the file then filters in memory. PostgreSQL's filtered read (0.222 s) pushes `WHERE status = 'DELIVERED'` into the query planner, so only matching rows cross the network — but the table still has to be scanned because there's no index on `status`. A B-tree index on `status` would let PostgreSQL skip non-matching rows entirely, which would matter a lot at larger scale.

**4. Why is JSON Lines more pipeline-friendly than one giant JSON array?**
A single JSON array must be parsed as one document — you can't read record 1 without loading the rest. JSON Lines is one JSON object per line: you can read, write, and process it one record at a time, append without rewriting the file, and stream it through a pipeline that never holds the whole dataset in memory. That's why log pipelines, ingestion tools, and event systems default to JSONL.

**5. What happens if a partition key has extremely high cardinality or poor query locality?**
High cardinality (e.g. partitioning by `order_id`) creates tens of thousands of tiny files — one per key — which is much worse than a single file. Every query pays filesystem metadata overhead, and most query engines struggle with the "small files problem". Poor locality means partitions don't line up with the filters people actually use, so pruning never kicks in and every query scans every partition. Good partition keys have moderate cardinality and match the most common `WHERE` clauses.

## Goal 4 — Airflow

### Evidence
| Artifact | Path |
|---|---|
| DAG list (unpaused) | [dag_list.png](evidence/goal4/dag_list.png) |
| Trigger dialog — full | [trigger_dialog_full.png](evidence/goal4/trigger_dialog_full.png) |
| Trigger dialog — partition | [trigger_dialog_partition.png](evidence/goal4/trigger_dialog_partition.png) |
| Full run grid (all green) and graph | [full_run_graph.png](evidence/goal4/full_run_graph.png) |
| Extract task log with run_id | [full_run_extract_log.png](evidence/goal4/full_run_extract_log.png) |
| Audit after full run | [audit_pipeline_runs_full.txt](evidence/goal4/audit_pipeline_runs_full.txt) |
| Curated run_ids | [curated_run_ids.txt](evidence/goal4/curated_run_ids.txt) |
| Partition load log | [partition_load_log.png](evidence/goal4/partition_load_log.png) |
| Partition loads after DAG | [partition_loads_after_dag.txt](evidence/goal4/partition_loads_after_dag.txt) |
| Failure grid | [failure_grid.png](evidence/goal4/failure_grid.png) |
| Failure extract log | [failure_extract_log.png](evidence/goal4/failure_log.png) |
| Failure retry detail | [failure_retry_detail.png](evidence/goal4/failure_retry_detail.png) |
| Recovery grid | [recovery_grid.png](evidence/goal4/recovery_grid.png) |
| Dedup after recovery | [dedup_after_recovery.txt](evidence/goal4/dedup_after_recovery.txt) |

### Design decision — where failure state lives

`audit.pipeline_runs` records only runs that reach the **load** stage. It is a table about **loaded data lineage**, not a general job-execution log.

- A successful `load` or `load-partition` writes/updates its row with `status = SUCCESS` and the affected-row count.
- A failure **before** load (missing source file, connection error, etc.) is not recorded in `audit.pipeline_runs`. Nothing was loaded, so there is no data to trace.

Failure evidence for the pipeline therefore lives in **Airflow's metadata database**, which is the authoritative source for *job execution* state:

- The Grid and Graph views show which task failed, when, and after how many retries.
- The task log shows the exact error and the `[FAILURE]` line printed by `on_failure_callback` (task_id, run_id, try_number, exception type).
- Airflow's own `task_instance` and `dag_run` tables retain retry counts and timestamps.

This is a deliberate separation:

| Question | Answered by |
|---|---|
| Which rows are loaded, from which run? | `audit.pipeline_runs`, `curated.sales_order_lines.pipeline_run_id` |
| Did the job run? Did it fail? How many times did it retry? | Airflow UI (Grid, Graph, task logs) |
| Why did it fail? | Airflow task log + `on_failure_callback` output |

Merging the two would couple the data-lineage table to the orchestrator that happens to be running the pipeline today. If the orchestrator changes (cron → Airflow → Prefect → Dagster), `audit.pipeline_runs` would need schema changes each time. Keeping execution state in the orchestrator keeps the audit table portable.

### Task A — Airflow initialization
Airflow runs as three services in Docker Compose: `airflow-init` (one-shot: `db migrate` + create admin user), `airflow-webserver` (UI on `localhost:8080`), and `airflow-scheduler` (triggers runs, tracks retries). All three share the same Postgres service used by the pipeline — the metadata lives in the `airflow` database, separate from `dss150p`.

### Task B — DAG operational configuration

| Requirement | Implementation |
|---|---|
| Schedule | `schedule='0 2 * * *'` — daily at 02:00 UTC |
| Parameters | `run_mode` (enum: `full`/`partition`), `year`, `month` (1..12) |
| Dependencies | `extract >> transform >> load >> validate` |
| Retries | `retries=2`, `retry_delay=timedelta(minutes=1)` |
| Timeout | `execution_timeout=timedelta(minutes=10)` per task; `dagrun_timeout=timedelta(minutes=30)` for the whole DAG |
| Failure handling | `on_failure_callback` prints `task_id`, `run_id`, `try_number`, `logical_date`, error type, and exception |
| Catch-up | `catchup=False` |
| Business logic separation | DAG calls `python -m src.cli ...`; no transformation code in the DAG file |
| Run identity | `PIPELINE_RUN_ID="{{ run_id }}"` exported to every task |

**Why daily at 02:00 UTC:** the source files are exported from the upstream system once per day, and the analytics consumers expect fresh numbers each morning. 02:00 UTC is after the upstream export completes and before the analysts' workday starts.

**Why `catchup=False`:** the source files are current-state exports, not historical batches. If the scheduler were offline for a week, a catch-up would fire seven identical full loads of the current state — wasted work, and seven redundant rows in `audit.pipeline_runs`. Backfill is an explicit decision (manual trigger with `year`/`month` params), not an automatic one.

**Time zone note:** the scheduled run in the audit table shows `logical_date=2026-09-20T02:00:00+00:00` but `started_at_utc=2026-09-21 13:51:37`. Airflow's *logical date* represents the interval the run is scheduled for; the *start time* is when the scheduler actually executed it. In production these would be seconds apart; on a laptop that had been off, the gap is larger. `catchup=False` meant Airflow ran exactly one backlogged interval, not all of them.

### Task C — Full run
Triggered manually from the UI with default parameters (`run_mode=full`). All four tasks reached `success`. Evidence: [full_run_graph.png](evidence/goal4/full_run_graph.png).

`audit.pipeline_runs` after the run:

| pipeline_run_id | status | rows_curated | started_at_utc |
|---|---|---|---|
| `manual__2026-09-21T13:52:32+00:00` | SUCCESS | 0 | 2026-09-21 13:53:38 |
| `scheduled__2026-09-20T02:00:00+00:00` | SUCCESS | 0 | 2026-09-21 13:51:37 |

Both runs reported `rows_curated=0` because the data was already loaded by Goal 2; the UPSERT skipped every row. That's the correct behavior — the pipeline recognized that nothing had changed.

**Why `curated.sales_order_lines.pipeline_run_id` still shows the old Goal 2 run id:** the row's `pipeline_run_id` column identifies **the transform run that produced the row**, not the load event. Airflow's `manual__...` run id appears in `audit.pipeline_runs` and in the task logs, but the business row was correctly left untouched because the content hadn't changed. This is the same design principle as `record_hash`: the data lineage of a row is set when the row is produced, not when it is transported.

### Task D — Parameterized partition run
Triggered with `run_mode=partition, year=2026, month=1`. The `load` task's bash takes the partition branch: it runs `python -m src.cli partition` to (re-)materialize the partitioned Parquet, then `python -m src.cli load-partition --year 2026 --month 1`. The load reported `0` rows affected (identical to what was already there), and `audit.partition_loads` for `2026-01` now shows the Airflow run id:

| partition_key | row_count | pipeline_run_id |
|---|---|---|
| `2026-01` | 2504 | `manual__...` (latest Airflow run) |

`row_count` reflects the partition's total size (2504), not this run's affected-row count, so the audit row is stable across reruns.

### Task E — Deliberate failure and recovery

**Setup:** `data/source/orders.csv` was renamed to `orders.csv.bak` on the host. Because the repo is bind-mounted into the Airflow container at `/opt/airflow/project`, the change was immediately visible to the DAG.

**Observed behavior:**

- `extract` attempted to run, and — on each attempt — the pipeline raised `PipelineStageError: [extract] run_id=manual__2026-09-21T14:05:26+00:00: Missing source file: /opt/airflow/project/data/source/orders.csv`.
- The task **retried twice** with ~1-minute delays between attempts (retry_delay=1 min). Total: 3 attempts (1 initial + 2 retries) — matching `retries=2`.
- After the third failure, the task entered `failed` state. Airflow's `on_failure_callback` fired and printed:
  `[FAILURE] dag_id=dss150p_sales_pipeline task_id=extract run_id=manual__2026-09-21T14:05:26+00:00 try_number=3 logical_date=2026-09-21 14...`
- `transform`, `load`, and `validate` never ran — they were marked `upstream_failed` (orange).
- `audit.pipeline_runs` was **not** written for the failed run. Nothing was loaded, so nothing appeared in the data-lineage table. Execution state lives in Airflow's metadata DB (Grid, Graph, task logs), which is the authoritative source for job-execution history.

**Recovery:** `data/source/orders.csv` was restored, and the failed `extract` task was **cleared** from the Airflow UI. Airflow re-ran the same DAG run: `extract` succeeded on the fourth attempt (visible as "Tries: 1, 2, 3 red; 4 green"), and the downstream tasks followed. The recovery run's `extract` log shows the **same `run_id`** as the failed attempts:

```
[extract] run_id=manual__2026-09-21T14:05:26+00:00 raw_dir=/opt/airflow/project/data/raw/run_id=manual__2026-09-21T14:05:26+00:00
Command exited with return code 0
```

**Verification that recovery was clean:**

```sql
SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders
  FROM curated.sales_order_lines;
-- 49834 | 49834
```

No duplicates, no orphan rows, and no manual database cleanup was required. The same UPSERT guard (`record_hash IS DISTINCT FROM EXCLUDED.record_hash`) handled it.

**Which steps are safe to rerun and why:** every stage is idempotent, so all four are safe to rerun:

- **extract** — writes a new `run_id=<id>/` folder. The previous run's snapshot is untouched, and source files are never modified.
- **transform** — reads the raw snapshot for the given `run_id` and writes staging/curated parquet. Same input → same output. Re-running with the same `run_id` overwrites the same files with identical bytes; re-running with a new `run_id` creates a fresh, non-conflicting snapshot.
- **load** — uses `ON CONFLICT (order_id) DO UPDATE ... WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash`. Rows whose content hasn't changed are skipped, so a rerun never duplicates or rewrites unchanged rows.
- **validate** — pure read-only function over the curated parquet; no side effects.

The only "unsafe" action would be editing source files in place. That's why the raw layer exists: every run reads its own immutable snapshot, so reruns never have to fight with each other or with upstream file changes. In the failure experiment, `Clear + rerun` was safe precisely because of these properties — recovery succeeded with no duplicate `order_id`s and no manual DB cleanup.

### Task (optional) — Backfill reasoning

To backfill a historical month, e.g. 2025-03, on a DAG that normally runs daily:

1. Identify the target partition: `year=2025, month=3`.
2. Confirm the transform step for that interval produces the correct `order_timestamp` range. Because `transform` reads the current source files and filters by `order_year`/`order_month` during the `partition` step, backfilling relies on the source file still containing that historical data.
3. Trigger a manual DAG run with `run_mode=partition, year=2025, month=3`. The `load` task routes to `load-partition`, which upserts only the 2025-03 partition.
4. **Idempotency:** the UPSERT guard means a re-trigger of the same partition changes nothing if the source data hasn't changed. This is what allows a backfill to be retried safely.
5. **Avoiding double loads:** there's no risk of duplicate `order_id`s because the primary key prevents them. The only risk would be overwriting good data with stale data if the source file had been modified — that's why the transform step must produce the correct row content for that interval before the load runs.

If the source files were historical snapshots (one per day) rather than a current-state export, the correct pattern would be to point the pipeline at the correct historical snapshot before triggering. That's out of scope for this lab, where source files represent the current state.

## Technical Reflection

### Modularity
The pipeline is split into five responsibilities — extract, transform, load, validate, benchmark — with each stage isolated in its own module. `src/cli.py` is a thin dispatcher; it knows *what* to call but not *how* anything works. That separation paid off three times: when Airflow needed to drive the same pipeline, the DAG just called `python -m src.cli ...`; when the container needed to run the same code as the host, no changes were required; and when a bug appeared in `load-partition`, the fix was contained to one function. If business logic lived in the CLI or DAG, each of those situations would have required rewriting code in multiple places.

### Idempotency
Two independent mechanisms make reruns safe. First, `record_hash` is deterministic — it hashes business columns only, deliberately excluding `pipeline_run_id` and `processed_at_utc`, so the same input always produces the same hash regardless of when the pipeline runs. Second, the PostgreSQL UPSERT is guarded by `WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash`, so a row whose content is unchanged is skipped entirely. This is why re-running `load` reports `0`, why the same partition can be loaded twice without creating duplicates, and why the failure-recovery scenario in Goal 4 needed no manual cleanup. Without these two pieces working together, retries and re-triggers would silently duplicate data.

### Storage trade-offs
The four formats measured in Goal 3 each won on a different axis. Parquet was smallest (5.5 MB) and fastest to read (0.06 s), because columnar layout compresses well and only the needed columns are read. CSV was second-smallest and fastest to *write* for its size, but has no schema — every read re-infers types. JSONL was 5× larger than Parquet because field names are repeated on every row, but it streams naturally (one record per line), which is why it's the format of choice for append-oriented pipelines. PostgreSQL was competitive on filtered queries because the `WHERE` clause runs inside the database, but its full-read is slow from Python because every row crosses the wire. No single format is best; the right choice depends on whether the workload is analytical scan, append log, or transactional query.

### Orchestration vs business logic
Airflow's job is to decide *when* and *in what order* things run, and to record what happened. It does not transform data — the DAG file contains zero business rules. This is why the same pipeline works whether it's triggered by a person, by Docker, or by Airflow's scheduler: the code that does the work is the same code, and the only thing that changes is who calls it. Merging the two would couple transformation logic to Airflow's Python API, making it untestable outside Airflow and impossible to run from cron or a container. The separation is what makes the pipeline portable.

### What would change at production scale
- **Raw snapshots** would move to object storage (S3/GCS) instead of local disk, and the manifest would become a table for queryability.
- **Benchmarks** would run on a fixed reference machine in CI, not on a developer laptop, so numbers are comparable across commits.
- **Failures** would alert to a channel (Slack/PagerDuty) from the `on_failure_callback` rather than only printing to the task log.
- **Partitioning** would shift to a date column with coarser granularity (month is already good; daily might be too fine at 50k rows).
- **Airflow** would run on a managed deployment (MWAA, Cloud Composer) with a real scheduler uptime, so `0 2 * * *` fires on time instead of being a laptop-dependent schedule.

## Section 15 — Technical Questions

**1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?**

`record_hash` gives each row a deterministic fingerprint of its business content. When the pipeline reruns and produces the same content, the hash is identical, so the UPSERT's `WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash` guard skips the row. Without it, every rerun would rewrite all 49,834 rows even though nothing changed — wasted I/O, unnecessary row versions, and misleading update timestamps.

The columns that must **not** be in the hash are the ones that change on every run but say nothing about the business content: `pipeline_run_id` (different run id each time) and `processed_at_utc` (different timestamp each time). If either were included, identical data would produce a different hash on every run, and the guard would never fire — the whole mechanism would be a no-op. In our implementation the hash covers only the business columns plus `source_updated_at`, which is meaningful because it's the timestamp from the *source system*, not from our pipeline.

**2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?**

Because transformation rules change. A bug in the staging code, a new business rule, or a different definition of "active customer" all mean you need to re-process from the original data. If raw is gone, you can't. Raw is also the only source of truth when a curated number looks wrong — you can trace exactly which input row produced it. In this lab, every `extract` writes a new `data/raw/run_id=<id>/` with a SHA-256 manifest, so any historical snapshot can be re-processed later.

**3. What is the difference between a data-quality rejection and a system exception?**

A **data-quality rejection** is expected: the input has a defect (missing email, negative price, bad status, orphan foreign key). The pipeline continues, records the row in quarantine with a reason, and moves on. A **system exception** is unexpected: a missing file, a database connection failure, a schema mismatch. The pipeline stops, raises an error, and exits non-zero so the scheduler can react. Conflating them causes problems — treating a bad row as a system failure would halt the pipeline for a single record, and treating a missing database as a data problem would let the pipeline continue while producing garbage.

**4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?**

Three structural reasons. Parquet is **columnar**, so a query that only needs `order_timestamp` and `net_amount` reads only those two columns from disk — CSV must read every byte of every row. Parquet is **compressed** (snappy in our case), so fewer bytes cross the disk bus. Parquet stores **typed values**, so numbers are read as numbers rather than parsed from strings on every read — CSV parsing at 50k rows is measurable, and at 50M rows it dominates. The trade-off is that Parquet is not human-readable and is harder to append to incrementally.

**5. Why is a DAG that contains all transformation logic directly considered harder to maintain?**

Three reasons. First, the logic becomes untestable outside Airflow — you can't run a unit test on a transformation that only exists inside a BashOperator's command string. Second, it's not reusable — running the same logic from a cron job, a container, or a Jupyter notebook requires copy-pasting it. Third, changes to transformation logic create DAG parse risk — a syntax error in a transformation function that runs at DAG parse time can take down the whole scheduler. In our design, the DAG calls `python -m src.cli <stage>`, and every stage is a normal Python function that can be tested, reused, and deployed independently of Airflow.

**6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.**

Retries assume the underlying operation is safe to repeat. If it isn't, a retry after a *partial* failure can double the effect. Example: a task that appends new orders to `curated.sales_order_lines` without a conflict key. It inserts 20,000 rows, then crashes on row 20,001 due to a network blip. Airflow retries. The task starts from scratch and inserts all 49,834 rows — now the table has 69,834 rows, with 20,000 duplicates. Downstream aggregates are wrong, and cleanup requires a manual DELETE. In our pipeline, this can't happen because every write goes through an UPSERT keyed on `order_id` and guarded by `record_hash`. A retry of an already-completed load is a no-op — `upserted_rows=0`.

**7. What trade-off is introduced by partitioning too aggressively?**

Two costs. **Small-files problem:** each partition becomes at least one file. If you partition by a high-cardinality key (e.g. `order_id`), you end up with 49,834 files of one row each — the filesystem metadata overhead dwarfs the data, and every query pays open/close costs per file. **Poor pruning:** partitioning only helps if queries filter on the partition keys. If nobody queries by `order_month`, the partitioning is pure cost. A good partition key has moderate cardinality (10s–1000s of values), matches common `WHERE` clauses, and produces files of at least a few MB each. Our `order_year/order_month` split gives 30–40 partitions of ~1,500 rows each — reasonable for this dataset size.

**8. How would you adapt the pipeline if the source became an API or database instead of local files?**

Three changes, none of which touch the staging/curated/load logic. First, `src/extract/files.py` would be replaced with an API or DB client that writes the same `data/raw/run_id=<id>/` snapshot format — the downstream contract is unchanged. Second, the manifest would record the API endpoint and query parameters (or a DB query hash) alongside the SHA-256, since those are the "provenance" of an API-sourced snapshot. Third, error handling would shift: a network timeout is a **system exception** and should retry, whereas an API returning 404 for a specific record is a **data-quality rejection** and should quarantine. Everything from staging onward stays identical because it operates on `data/raw/<run_id>/`, not on the source itself.