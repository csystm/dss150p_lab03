"""Benchmark CSV, JSON Lines, Parquet, and PostgreSQL on the same curated dataset.

Same logical rows in all four; only the physical representation differs.
Reads repeat N times and report the median, per spec §9.2.
"""
from __future__ import annotations

import io
import os
import time
from pathlib import Path

import pandas as pd
import psycopg
from psycopg import sql

from src.config import DB, SETTINGS
from src.common.audit import utc_now_iso


# small utilities

def _median(values):
    s = sorted(v for v in values if v is not None)
    if not s:
        return float('nan')
    n = len(s)
    m = n // 2
    return s[m] if n % 2 else (s[m - 1] + s[m]) / 2


def _timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return time.perf_counter() - t0, result


def _connect():
    return psycopg.connect(
        host=DB['host'], port=DB['port'],
        dbname=DB['dbname'], user=DB['user'], password=DB['password'],
    )

# writers

def _write_csv(df, path):
    df.to_csv(path, index=False)


def _write_jsonl(df, path):
    # orient='records', lines=True => one JSON object per line (JSON Lines)
    df.to_json(path, orient='records', lines=True)


def _write_parquet(df, path):
    # snappy is the standard default; explicit here so the choice is visible
    df.to_parquet(path, index=False, compression='snappy')


def _db_write_time(df, repeats: int = 1) -> float:
    """Time COPY into a TEMP table shaped like curated.sales_order_lines.

    We do not touch the real curated table (already loaded); this measures
    the cost of getting the same logical rows into PostgreSQL.
    """
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    csv_text = buf.getvalue()
    cols_sql = sql.SQL(', ').join(sql.Identifier(c) for c in df.columns)

    best = None
    for _ in range(repeats):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute('CREATE TEMP TABLE bench_sol '
                        '(LIKE curated.sales_order_lines)')
            t0 = time.perf_counter()
            with cur.copy(
                sql.SQL('COPY bench_sol ({}) FROM STDIN WITH (FORMAT csv)')
                    .format(cols_sql)
            ) as copy:
                copy.write(csv_text)
            dt = time.perf_counter() - t0
            cur.execute('DROP TABLE bench_sol')
        best = dt if best is None else min(best, dt)
    return best


# readers

def _read_csv(path):
    return pd.read_csv(path)


def _read_jsonl(path):
    return pd.read_json(path, lines=True)


def _read_parquet(path):
    return pd.read_parquet(path)


def _time_pandas_reads(read_fn, path, status, repeats):
    """Return (median full-read, median read+filter). Both include I/O."""
    full, filt = [], []
    for _ in range(repeats):
        dt, _ = _timed(lambda: read_fn(path))
        full.append(dt)
    for _ in range(repeats):
        def op():
            data = read_fn(path)
            return data[data['status'] == status]
        dt, _ = _timed(op)
        filt.append(dt)
    return _median(full), _median(filt)


def _db_full_read():
    with _connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT * FROM curated.sales_order_lines')
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _db_filtered_read(status):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            'SELECT * FROM curated.sales_order_lines WHERE status = %s',
            (status,),
        )
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _time_db_reads(status, repeats):
    full, filt = [], []
    for _ in range(repeats):
        dt, _ = _timed(_db_full_read)
        full.append(dt)
    for _ in range(repeats):
        dt, _ = _timed(lambda: _db_filtered_read(status))
        filt.append(dt)
    return _median(full), _median(filt)


def _db_table_size_bytes() -> int:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_total_relation_size('curated.sales_order_lines')"
        )
        return int(cur.fetchone()[0])


def _db_row_count() -> int:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM curated.sales_order_lines')
        return int(cur.fetchone()[0])


# public entry

def run_benchmark(curated_path, output_dir, repeats: int = 5) -> Path:
    """Materialize the curated dataset as CSV, JSONL, Parquet, PostgreSQL.

    Measures size, write time, full-read median, filtered-read median.
    Writes benchmark_results.csv (long format) to output_dir.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(curated_path)
    row_count = len(df)
    status_filter = SETTINGS['storage_benchmark']['filter_status']

    # file paths
    csv_path     = output_dir / 'sales_order_lines.csv'
    jsonl_path   = output_dir / 'sales_order_lines.jsonl'
    parquet_path = output_dir / 'sales_order_lines.parquet'

    # write each format once; capture size + write time
    csv_write,     _ = _timed(lambda: _write_csv(df, csv_path))
    jsonl_write,   _ = _timed(lambda: _write_jsonl(df, jsonl_path))
    parquet_write, _ = _timed(lambda: _write_parquet(df, parquet_path))
    db_write = _db_write_time(df)

    csv_size     = os.path.getsize(csv_path)
    jsonl_size   = os.path.getsize(jsonl_path)
    parquet_size = os.path.getsize(parquet_path)
    db_size      = _db_table_size_bytes()

    # read timings
    csv_full,     csv_filt     = _time_pandas_reads(_read_csv,     csv_path,     status_filter, repeats)
    jsonl_full,   jsonl_filt   = _time_pandas_reads(_read_jsonl,   jsonl_path,   status_filter, repeats)
    parquet_full, parquet_filt = _time_pandas_reads(_read_parquet, parquet_path, status_filter, repeats)
    db_full,      db_filt      = _time_db_reads(status_filter, repeats)

    # sanity: same row count everywhere
    db_rows = _db_row_count()
    if db_rows != row_count:
        raise RuntimeError(
            f'Row count mismatch: files={row_count} db={db_rows}. '
            f'Run `python -m src.cli load` first.'
        )

    # assemble
    results = pd.DataFrame([
        dict(format='CSV',        size_bytes=csv_size,
             write_sec=csv_write,     full_read_median_sec=csv_full,
             filtered_read_median_sec=csv_filt,     row_count=row_count),
        dict(format='JSONL',      size_bytes=jsonl_size,
             write_sec=jsonl_write,   full_read_median_sec=jsonl_full,
             filtered_read_median_sec=jsonl_filt,   row_count=row_count),
        dict(format='Parquet',    size_bytes=parquet_size,
             write_sec=parquet_write, full_read_median_sec=parquet_full,
             filtered_read_median_sec=parquet_filt, row_count=row_count),
        dict(format='PostgreSQL', size_bytes=db_size,
             write_sec=db_write,      full_read_median_sec=db_full,
             filtered_read_median_sec=db_filt,      row_count=row_count),
    ])
    results['repeats'] = repeats
    results['filter_status'] = status_filter
    results['measured_at_utc'] = utc_now_iso()

    out_path = output_dir / 'benchmark_results.csv'
    results.to_csv(out_path, index=False)
    return out_path


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    raise NotImplementedError('Task C — will implement next')
