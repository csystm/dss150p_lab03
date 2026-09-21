"""Rerun-safe loading into PostgreSQL.

- curated.sales_order_lines UPSERT keyed on order_id
- Rows whose record_hash matches the stored row are skipped (no update)
- audit.pipeline_runs tracks each run's status and row counts
- audit.partition_loads records selected-partition loads (Goal 3)
"""
from __future__ import annotations

import pandas as pd
import psycopg
from psycopg import sql

from src.config import DB
from src.common.audit import utc_now_iso


CURATED_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'customer_city', 'customer_tier',
    'product_name', 'category', 'brand',
    'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount',
    'status',
    'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
]


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=DB['host'],
        port=DB['port'],
        dbname=DB['dbname'],
        user=DB['user'],
        password=DB['password'],
    )


def _upsert_statement():
    update_cols = [c for c in CURATED_COLUMNS if c != 'order_id']
    set_clause = sql.SQL(', ').join(
        sql.SQL('{c} = EXCLUDED.{c}').format(c=sql.Identifier(c))
        for c in update_cols
    )
    cols_sql = sql.SQL(', ').join(sql.Identifier(c) for c in CURATED_COLUMNS)
    placeholders = sql.SQL(', ').join(
        sql.Placeholder() * len(CURATED_COLUMNS)
    )
    return sql.SQL(
        'INSERT INTO curated.sales_order_lines ({cols}) VALUES ({vals}) '
        'ON CONFLICT (order_id) DO UPDATE SET {set_clause} '
        'WHERE curated.sales_order_lines.record_hash '
        '      IS DISTINCT FROM EXCLUDED.record_hash'
    ).format(cols=cols_sql, vals=placeholders, set_clause=set_clause)


def _record_run_start(run_id: str) -> None:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.pipeline_runs
              (pipeline_run_id, started_at_utc, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (pipeline_run_id) DO UPDATE
              SET started_at_utc = EXCLUDED.started_at_utc,
                  status = EXCLUDED.status,
                  completed_at_utc = NULL,
                  message = NULL
            """,
            (run_id, utc_now_iso(), 'RUNNING'),
        )


def _record_run_end(run_id: str, status: str, rows_curated: int, message: str | None = None) -> None:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE audit.pipeline_runs
               SET status = %s,
                   completed_at_utc = %s,
                   rows_curated = %s,
                   message = %s
             WHERE pipeline_run_id = %s
            """,
            (status, utc_now_iso(), rows_curated, message, run_id),
        )


def upsert_curated(df: pd.DataFrame, run_id: str) -> int:
    """Rerun-safe UPSERT into curated.sales_order_lines.

    Returns the number of rows actually inserted or updated. Rows whose
    record_hash equals the stored row are no-ops and are not counted.
    """
    if df is None or len(df) == 0:
        _record_run_start(run_id)
        _record_run_end(run_id, 'SUCCESS', 0, 'empty input')
        return 0

    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df[CURATED_COLUMNS].itertuples(index=False, name=None)
    ]

    _record_run_start(run_id)
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.executemany(_upsert_statement(), rows)
            affected = cur.rowcount
        _record_run_end(run_id, 'SUCCESS', affected)
        return affected
    except Exception as e:
        _record_run_end(run_id, 'FAILED', 0, str(e)[:1000])
        raise


def load_partition(df: pd.DataFrame, year: int, month: int, run_id: str) -> int:
    """Load only the selected year/month partition and record audit.partition_loads."""
    raise NotImplementedError('Implement Goal 3 selected-partition load')
