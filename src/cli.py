import argparse
import sys
import traceback
from pathlib import Path

from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id
from src.common.errors import PipelineStageError

def _run_stage(stage: str, run_id: str, fn, *args, **kwargs):
    """Call a stage function, converting any exception into PipelineStageError."""
    try:
        return fn(*args, **kwargs)
    except PipelineStageError:
        raise
    except Exception as e:
        raise PipelineStageError(stage, run_id, str(e)) from e

def _cmd_extract(run_id: str) -> Path:
    from src.extract.files import extract_sources
    return extract_sources(run_id)


def _cmd_transform(run_id: str):
    import pandas as pd
    from src.transform.staging import build_staging
    from src.transform.curated import build_curated

    raw_dir = path_for('raw_dir') / f'run_id={run_id}'
    staging, staging_q = build_staging(raw_dir, run_id)

    curated, orphan_q = build_curated(staging, run_id)

    curated_dir = path_for('curated_dir')
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated_path = curated_dir / 'sales_order_lines.parquet'
    curated.to_parquet(curated_path, index=False)

    quarantine_dir = path_for('quarantine_dir') / f'run_id={run_id}'
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    parts = [q for q in (staging_q, orphan_q)
             if q is not None and len(q) > 0]
    combined = pd.concat(parts, ignore_index=True) if parts else staging_q
    combined.to_parquet(quarantine_dir / 'quarantine.parquet', index=False)

    return curated_path


def _cmd_load(run_id: str) -> int:
    import pandas as pd
    from src.load.postgres import upsert_curated

    curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
    df = pd.read_parquet(curated_path)
    return upsert_curated(df, run_id)


def _cmd_validate() -> list[str]:
    import pandas as pd
    from src.validate.quality import validate_curated

    curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
    df = pd.read_parquet(curated_path)
    return validate_curated(df)


def _cmd_benchmark(repeats: int, run_id: str):
    from src.benchmark.storage import run_benchmark
    curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
    out_dir = path_for('benchmark_dir')
    out_dir.mkdir(parents=True, exist_ok=True)
    return run_benchmark(curated_path, out_dir, repeats=repeats)


def _cmd_partition(run_id: str):
    import pandas as pd
    from src.benchmark.storage import write_partitioned_parquet

    curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
    df = pd.read_parquet(curated_path)
    return write_partitioned_parquet(df, path_for('partition_dir'))


def _cmd_load_partition(year: int, month: int, run_id: str) -> int:
    import pandas as pd
    from src.benchmark.storage import write_partitioned_parquet
    from src.load.postgres import load_partition

    partition_root = path_for('partition_dir')
    leaf = partition_root / f'order_year={year}' / f'order_month={month}'

    # Materialize the partition tree on demand if the requested leaf is missing.
    # This keeps load-partition self-contained (the Airflow DAG relies on the
    # same guarantee) and avoids a hard failure after a clean slate.
    if not leaf.is_dir():
        curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
        if not curated_path.is_file():
            raise FileNotFoundError(
                f'Curated parquet not found: {curated_path}. '
                f'Run `python -m src.cli run-all` first.'
            )
        df_curated = pd.read_parquet(curated_path)
        write_partitioned_parquet(df_curated, partition_root)

    df = pd.read_parquet(leaf)
    return load_partition(df, year, month, run_id)


def main() -> int:
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition')
    p.add_argument('--year', type=int, required=True)
    p.add_argument('--month', type=int, required=True)
    sub.add_parser('partition')
    sub.add_parser('run-all')

    args = parser.parse_args()
    run_id = new_run_id()

    try:
        if args.command == 'validate-env':
            print('PROJECT_ROOT=', PROJECT_ROOT)
            print('DB host/database=', DB['host'], DB['dbname'])
            print('Configured source=', SETTINGS['pipeline']['source_dir'])
            return 0

        if args.command == 'extract':
            raw = _run_stage('extract', run_id, _cmd_extract, run_id)
            print(f'[extract] run_id={run_id} raw_dir={raw}')
            return 0

        if args.command == 'transform':
            curated_path = _run_stage('transform', run_id, _cmd_transform, run_id)
            print(f'[transform] run_id={run_id} curated={curated_path}')
            return 0

        if args.command == 'load':
            n = _run_stage('load', run_id, _cmd_load, run_id)
            print(f'[load] run_id={run_id} upserted_rows={n}')
            return 0

        if args.command == 'validate':
            errors = _run_stage('validate', run_id, _cmd_validate)
            if errors:
                print(f'[validate] FAILED with {len(errors)} error(s):')
                for e in errors:
                    print(f'  - {e}')
                return 1
            print('[validate] OK')
            return 0

        if args.command == 'benchmark':
            result = _run_stage('benchmark', run_id, _cmd_benchmark, args.repeats, run_id)
            print(f'[benchmark] run_id={run_id} results={result}')
            return 0

        if args.command == 'partition':
            out = _run_stage('partition', run_id, _cmd_partition, run_id)
            print(f'[partition] run_id={run_id} output={out}')
            return 0

        if args.command == 'load-partition':
            n = _run_stage('load-partition', run_id, _cmd_load_partition,
                           args.year, args.month, run_id)
            print(f'[load-partition] year={args.year} month={args.month} rows={n}')
            return 0

        if args.command == 'run-all':
            raw = _run_stage('extract', run_id, _cmd_extract, run_id)
            print(f'[extract] run_id={run_id} raw_dir={raw}')
            curated_path = _run_stage('transform', run_id, _cmd_transform, run_id)
            print(f'[transform] run_id={run_id} curated={curated_path}')
            n = _run_stage('load', run_id, _cmd_load, run_id)
            print(f'[load] run_id={run_id} upserted_rows={n}')
            errors = _run_stage('validate', run_id, _cmd_validate)
            if errors:
                print(f'[validate] FAILED with {len(errors)} error(s):')
                for e in errors:
                    print(f'  - {e}')
                return 1
            print('[validate] OK')
            return 0

        raise ValueError(f'Unknown command: {args.command}')

    except NotImplementedError as e:
        print(f'[ERROR] stage not yet implemented: {e}', file=sys.stderr)
        return 2
    except Exception as e:
        print(f'[ERROR] command={args.command} run_id={run_id}: {e}', file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())