"""Staging layer: typing, normalization, dedup, validation, quarantine.

Rules (Goal 2 item 8.3):
- customers: dedup by customer_id (latest updated_at); lowercase+trim email;
  trim+title-case city; parse timestamps UTC; missing email -> quarantine.
- products: dedup by product_id (latest updated_at); flatten category.name /
  category.department; numeric unit_price; <=0 or NaN price -> quarantine.
- orders: dedup by order_id (latest updated_at); UTC timestamps; quantity int
  in [min,max]; status in allowed set; first failing check -> quarantine reason.

Every valid record gets pipeline_run_id and staged_at_utc.
Valid records -> data/staging/run_id=<run_id>/<dataset>.parquet
Invalid records -> data/quarantine/run_id=<run_id>/quarantine.parquet
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pandas as pd

from src.common.audit import utc_now_iso
from src.config import SETTINGS, path_for


QUARANTINE_COLUMNS = [
    'source', 'business_key', 'reason', 'raw',
    'pipeline_run_id', 'staged_at_utc',
]


#  helpers

def _parse_utc(series: pd.Series) -> pd.Series:
    """Parse to timezone-aware UTC. Naive values are treated as UTC."""
    return pd.to_datetime(series, utc=True, errors='coerce')


def _dedup_latest(df: pd.DataFrame, key: str, ts_col: str) -> pd.DataFrame:
    """Keep one row per key with max ts_col. Stable tiebreak: last in file order."""
    return (
        df.sort_values([key, ts_col], kind='mergesort')
          .drop_duplicates(subset=[key], keep='last')
          .reset_index(drop=True)
    )


def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    """Atomic parquet write: temp file in same dir, then os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix='.parquet.tmp')
    os.close(fd)
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _row_to_json(row: pd.Series) -> str:
    """Serialize a row to JSON, normalizing pandas/numpy scalars."""
    payload = {}
    for k, v in row.items():
        if v is None:
            payload[k] = None
            continue
        if isinstance(v, pd.Timestamp):
            payload[k] = v.isoformat()
            continue
        try:
            if pd.isna(v):
                payload[k] = None
                continue
        except (TypeError, ValueError):
            pass
        if hasattr(v, 'item') and not isinstance(v, (str, bytes)):
            try:
                payload[k] = v.item()
                continue
            except Exception:
                pass
        payload[k] = v
    return json.dumps(payload, default=str)


def _empty_quarantine() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype='string') for c in QUARANTINE_COLUMNS})


def _build_quarantine(
    source: str,
    key_col: str,
    rows: pd.DataFrame,
    reasons: pd.Series,
    drop_cols: list[str],
    run_id: str,
    staged_at: str,
) -> pd.DataFrame:
    if len(rows) == 0:
        return _empty_quarantine()
    view = rows.drop(columns=drop_cols, errors='ignore')
    payloads = view.apply(_row_to_json, axis=1)
    return pd.DataFrame({
        'source': pd.Series([source] * len(rows), dtype='string'),
        'business_key': rows[key_col].astype('string').reset_index(drop=True),
        'reason': pd.Series(list(reasons), dtype='string'),
        'raw': payloads.astype('string').reset_index(drop=True),
        'pipeline_run_id': pd.Series([run_id] * len(rows), dtype='string'),
        'staged_at_utc': pd.Series([staged_at] * len(rows), dtype='string'),
    })[QUARANTINE_COLUMNS]


# customers

def _stage_customers(raw: pd.DataFrame, run_id: str, staged_at: str):
    df = raw.copy()
    df['created_at'] = _parse_utc(df['created_at'])
    df['updated_at'] = _parse_utc(df['updated_at'])

    for col in ('customer_id', 'first_name', 'last_name', 'customer_tier'):
        df[col] = df[col].astype('string').str.strip()
    df['email'] = df['email'].astype('string').str.strip().str.lower()
    df['city']  = df['city'].astype('string').str.strip().str.title()

    df = _dedup_latest(df, 'customer_id', 'updated_at')

    invalid_mask = df['email'].isna() | (df['email'] == '')
    valid = df.loc[~invalid_mask].copy()
    invalid_rows = df.loc[invalid_mask].copy()

    q = _build_quarantine(
        source='customers',
        key_col='customer_id',
        rows=invalid_rows,
        reasons=pd.Series(['missing_email'] * len(invalid_rows), dtype='string'),
        drop_cols=[],
        run_id=run_id, staged_at=staged_at,
    )

    valid['pipeline_run_id'] = run_id
    valid['staged_at_utc'] = staged_at
    return valid.reset_index(drop=True), q


# products

def _stage_products(raw: pd.DataFrame, run_id: str, staged_at: str):
    df = raw.copy()
    df['category_name'] = df['category'].apply(
        lambda c: c.get('name') if isinstance(c, dict) else None)
    df['category_dept'] = df['category'].apply(
        lambda c: c.get('department') if isinstance(c, dict) else None)
    df = df.drop(columns=['category']).rename(
        columns={'category_name': 'category', 'category_dept': 'department'})

    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['updated_at'] = _parse_utc(df['updated_at'])
    df['active'] = df['active'].astype('boolean')

    for col in ('product_id', 'name', 'brand', 'category', 'department'):
        df[col] = df[col].astype('string').str.strip()

    df = _dedup_latest(df, 'product_id', 'updated_at')

    invalid_mask = df['unit_price'].isna() | (df['unit_price'] <= 0)
    valid = df.loc[~invalid_mask].copy()
    invalid_rows = df.loc[invalid_mask].copy()

    q = _build_quarantine(
        source='products',
        key_col='product_id',
        rows=invalid_rows,
        reasons=pd.Series(['invalid_price'] * len(invalid_rows), dtype='string'),
        drop_cols=[],
        run_id=run_id, staged_at=staged_at,
    )

    valid['pipeline_run_id'] = run_id
    valid['staged_at_utc'] = staged_at
    return valid.reset_index(drop=True), q


# orders

def _stage_orders(raw: pd.DataFrame, run_id: str, staged_at: str):
    allowed = set(SETTINGS['quality']['allowed_order_statuses'])
    qmin = int(SETTINGS['quality']['min_quantity'])
    qmax = int(SETTINGS['quality']['max_quantity'])

    df = raw.copy()
    df['order_timestamp'] = _parse_utc(df['order_timestamp'])
    df['updated_at']      = _parse_utc(df['updated_at'])

    df['quantity_num'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unit_price']   = pd.to_numeric(df['unit_price'], errors='coerce')
    df['discount_pct'] = pd.to_numeric(df['discount_pct'], errors='coerce')

    for col in ('order_id', 'customer_id', 'product_id', 'status'):
        df[col] = df[col].astype('string').str.strip()

    df = _dedup_latest(df, 'order_id', 'updated_at')

    status_bad = df['status'].isna() | ~df['status'].isin(allowed)
    qty_bad = (df['quantity_num'].isna()
               | (df['quantity_num'] < qmin)
               | (df['quantity_num'] > qmax))

    reason = pd.Series([None] * len(df), index=df.index, dtype='object')
    reason = reason.mask(status_bad, 'invalid_status')
    reason = reason.mask(~status_bad & qty_bad, 'invalid_quantity')
    invalid_mask = reason.notna()

    valid = df.loc[~invalid_mask].copy()
    valid['quantity'] = valid['quantity_num'].astype('int64')
    valid = valid.drop(columns=['quantity_num'])

    invalid_rows = df.loc[invalid_mask].copy()
    q = _build_quarantine(
        source='orders',
        key_col='order_id',
        rows=invalid_rows,
        reasons=reason.loc[invalid_mask],
        drop_cols=['quantity_num'],
        run_id=run_id, staged_at=staged_at,
    )

    valid['pipeline_run_id'] = run_id
    valid['staged_at_utc']   = staged_at
    return valid.reset_index(drop=True), q


# public entry

def build_staging(raw_dir, run_id: str):
    """Clean, type, dedup, and validate all three source datasets.

    Returns (staging: dict[str, DataFrame], quarantine: DataFrame).
    Also writes staging parquet files and the combined quarantine parquet.
    """
    raw_dir = Path(raw_dir)
    if not raw_dir.is_dir():
        raise FileNotFoundError(f'Raw directory not found: {raw_dir}')

    staged_at = utc_now_iso()
    staging_dir = path_for('staging_dir') / f'run_id={run_id}'
    quarantine_dir = path_for('quarantine_dir') / f'run_id={run_id}'

    customers_raw = pd.read_csv(raw_dir / 'customers.csv')
    orders_raw    = pd.read_csv(raw_dir / 'orders.csv')
    with (raw_dir / 'products.json').open(encoding='utf-8') as f:
        products_raw = pd.DataFrame(json.load(f))

    c_valid, c_q = _stage_customers(customers_raw, run_id, staged_at)
    p_valid, p_q = _stage_products(products_raw,  run_id, staged_at)
    o_valid, o_q = _stage_orders(orders_raw,      run_id, staged_at)

    staging = {'customers': c_valid, 'products': p_valid, 'orders': o_valid}
    for name, df in staging.items():
        _write_parquet_atomic(df, staging_dir / f'{name}.parquet')

    frames = [q for q in (c_q, p_q, o_q) if len(q) > 0]
    quarantine = (pd.concat(frames, ignore_index=True)
                  if frames else _empty_quarantine())
    _write_parquet_atomic(quarantine, quarantine_dir / 'quarantine.parquet')

    return staging, quarantine
