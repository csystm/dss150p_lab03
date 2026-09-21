"""DSS150P sales pipeline DAG.

Coordination only — no business logic lives here. Every task invokes a thin
CLI entry point implemented in src/. The same pipeline_run_id (Airflow's
DAG run_id) is exported to every task so audit.pipeline_runs and
curated.sales_order_lines.pipeline_run_id stay consistent across the run.

Operational choices:

- schedule='0 2 * * *' — daily at 02:00 UTC. Source files are daily exports
  from the upstream commerce system; analysts expect fresh numbers each morning.
- catchup=False — the source files are current-state exports, not historical
  batches. Replaying missed intervals would reload the same data multiple times
  and pollute audit tables. Backfill is an explicit, manual decision.
- retries=2, 1-minute delay — transient file locks or DB connection blips are
  the common failure modes; two quick retries handle them without long waits.
- execution_timeout=10m — guards against a hang (e.g. stuck DB connection).
  The whole pipeline normally finishes in seconds on ~50k rows.
- on_failure_callback — records task_id, run_id, try_number, and the exception
  so the failure is diagnosable from the Airflow log alone.

Parameters (set per DAG run via the UI Trigger form):

- run_mode: 'full' | 'partition'
- year, month: used only when run_mode='partition'
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'


def failure_callback(context):
    """Write failure context so the task log alone is enough to debug."""
    ti = context['task_instance']
    dag_run = context['dag_run']
    err = context.get('exception')
    when = context.get('logical_date') or context.get('execution_date')
    print(
        f'[FAILURE] '
        f'dag_id={ti.dag_id} '
        f'task_id={ti.task_id} '
        f'run_id={dag_run.run_id} '
        f'try_number={ti.try_number} '
        f'logical_date={when} '
        f'error_type={type(err).__name__ if err else None} '
        f'error={err!r}'
    )


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
    'on_failure_callback': failure_callback,
}


def _cmd(cli_subcommand: str) -> str:
    """Wrap a CLI subcommand with cd + PIPELINE_RUN_ID from the DAG run_id."""
    return (
        f'cd {PROJECT} && '
        f'PIPELINE_RUN_ID="{{{{ run_id }}}}" '
        f'python -m src.cli {cli_subcommand}'
    )


# Load task bash: full mode loads everything; partition mode re-materializes
# the partitioned Parquet, then loads only the requested year/month partition.
LOAD_BASH = f'''
set -euo pipefail
cd {PROJECT}
export PIPELINE_RUN_ID="{{{{ run_id }}}}"

if [ "{{{{ params.run_mode }}}}" = "partition" ]; then
    echo "[dag] partition mode: year={{{{ params.year }}}} month={{{{ params.month }}}}"
    python -m src.cli partition
    python -m src.cli load-partition \
        --year {{{{ params.year }}}} \
        --month {{{{ params.month }}}}
else
    echo "[dag] full mode: loading all curated rows"
    python -m src.cli load
fi
'''.strip()


with DAG(
    dag_id='dss150p_sales_pipeline',
    description='DSS150P Lab 3 — modular data pipeline, coordinated by Airflow',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition'],
                          description='full: load all rows. partition: load one year/month.'),
        'year':     Param(2026, type='integer',
                          description='Partition year (used only if run_mode=partition).'),
        'month':    Param(1, type='integer', minimum=1, maximum=12,
                          description='Partition month 1..12 (used only if run_mode=partition).'),
    },
    tags=['DSS150P', 'lab3'],
) as dag:
    extract = BashOperator(
        task_id='extract',
        bash_command=_cmd('extract'),
    )
    transform = BashOperator(
        task_id='transform',
        bash_command=_cmd('transform'),
    )
    load = BashOperator(
        task_id='load',
        bash_command=LOAD_BASH,
    )
    validate = BashOperator(
        task_id='validate',
        bash_command=_cmd('validate'),
    )

    extract >> transform >> load >> validate