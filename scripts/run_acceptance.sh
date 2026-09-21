#!/usr/bin/env bash
# DSS150P Lab 3 — integrated acceptance test (section 11 of the lab spec).
# Runs the full pipeline end-to-end from a clean state and tees everything
# to docs/evidence/integrated_acceptance.txt with stage banners and timings.
#
# Usage:  bash scripts/run_acceptance.sh
# Requires: Docker Desktop running, .venv activated, .env present.

set -u  # don't use -e: we want to capture failures, not abort silently

OUT=docs/evidence/integrated_acceptance.txt
mkdir -p docs/evidence

log()  { printf '\n========================================\n%s\n========================================\n' "$*"; }
ts()   { date -u +%Y-%m-%dT%H:%M:%SZ; }

run() {
  local label="$1"; shift
  log "[$(ts)] ${label}"
  # tee combined stdout+stderr, capture exit code separately
  "$@" 2>&1 | tee -a "$OUT"
  local rc=${PIPESTATUS[0]}
  echo "[exit=${rc}]" | tee -a "$OUT"
  return $rc
}

# Fresh log file each time
: > "$OUT"
log "DSS150P Lab 3 — Integrated Acceptance Test — started $(ts)"

# --- Environment ---
run "python --version"                    python --version
run "pip freeze (key packages)"           bash -c "pip freeze | grep -Ei 'pandas|pyarrow|psycopg|numpy'"
run "validate-env (host)"                 python -m src.cli validate-env

# --- Postgres ---
run "docker compose up -d postgres"       docker compose up -d postgres
run "docker compose ps"                   docker compose ps
run "psql: \\dn"                          docker exec dss150p-postgres psql -U dss150p -d dss150p -c '\dn'
run "psql: \\dt curated.*"                docker exec dss150p-postgres psql -U dss150p -d dss150p -c '\dt curated.*'

# --- Pipeline ---
run "run-all (extract -> transform -> load -> validate)" python -m src.cli run-all
run "load (rerun; expect 0)"              python -m src.cli load
run "validate"                            python -m src.cli validate

# --- Benchmarks + partitioning ---
run "benchmark --repeats 5"               python -m src.cli benchmark --repeats 5
run "partition (materialize Parquet tree)" python -m src.cli partition
run "load-partition --year 2026 --month 1" python -m src.cli load-partition --year 2026 --month 1

# --- Post-run DB checks ---
run "psql: curated row counts"            docker exec dss150p-postgres psql -U dss150p -d dss150p -c \
  "SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders FROM curated.sales_order_lines;"
run "psql: audit.pipeline_runs"           docker exec dss150p-postgres psql -U dss150p -d dss150p -c \
  "SELECT pipeline_run_id, status, rows_curated, started_at_utc FROM audit.pipeline_runs ORDER BY started_at_utc DESC LIMIT 5;"
run "psql: audit.partition_loads"         docker exec dss150p-postgres psql -U dss150p -d dss150p -c \
  "SELECT partition_key, row_count, pipeline_run_id FROM audit.partition_loads ORDER BY loaded_at_utc DESC;"

# --- Airflow ---
run "airflow up"                          docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
run "airflow ps"                          docker compose -f docker-compose.yml -f docker-compose.airflow.yml ps

log "Integrated acceptance test finished $(ts)"
echo "Full log: $OUT"