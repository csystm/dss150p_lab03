"""Curated-layer assertions. Returns a list of human-readable errors.

Empty list means validation passed. Non-empty means the pipeline should fail
loudly so the orchestrator can react (never silently continue).
"""
from __future__ import annotations

import pandas as pd

from src.config import SETTINGS


REQUIRED_AUDIT_FIELDS = [
    'pipeline_run_id',
    'processed_at_utc',
    'record_hash',
    'source_updated_at',
]


def validate_curated(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []

    if df is None or len(df) == 0:
        return ['curated dataframe is empty']

    # Required columns present at all
    required_cols = [
        'order_id', 'customer_id', 'product_id',
        'quantity', 'unit_price', 'discount_pct',
        'gross_amount', 'discount_amount', 'net_amount',
        'status',
        *REQUIRED_AUDIT_FIELDS,
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        errors.append(f'missing required columns: {missing}')
        return errors  # downstream checks would KeyError

    # 1. order_id non-null and unique
    null_ids = int(df['order_id'].isna().sum())
    if null_ids:
        errors.append(f'order_id has {null_ids} null value(s)')

    dup_ids = int(df['order_id'].duplicated().sum())
    if dup_ids:
        dups = df.loc[df['order_id'].duplicated(keep=False), 'order_id'] \
                 .dropna().unique()[:5].tolist()
        errors.append(f'order_id has {dup_ids} duplicate row(s); '
                      f'sample: {dups}')

    # 2. quantity range
    qmin = int(SETTINGS['quality']['min_quantity'])
    qmax = int(SETTINGS['quality']['max_quantity'])
    q_out = df['quantity'].notna() & (
        (df['quantity'] < qmin) | (df['quantity'] > qmax)
    )
    if int(q_out.sum()):
        errors.append(f'quantity out of range [{qmin},{qmax}]: '
                      f'{int(q_out.sum())} row(s)')

    # 3. non-negative amounts
    for col in ('gross_amount', 'discount_amount', 'net_amount'):
        neg = df[col].notna() & (df[col] < 0)
        if int(neg.sum()):
            errors.append(f'{col} has {int(neg.sum())} negative value(s)')

    # 4. allowed statuses
    allowed = set(SETTINGS['quality']['allowed_order_statuses'])
    bad_status = df['status'].notna() & ~df['status'].isin(allowed)
    if int(bad_status.sum()):
        bad_vals = sorted(df.loc[bad_status, 'status'].unique().tolist())
        errors.append(f'status not in allowed set: {int(bad_status.sum())} '
                      f'row(s); values: {bad_vals}')

    # 5. required audit fields non-null
    for col in REQUIRED_AUDIT_FIELDS:
        n = int(df[col].isna().sum())
        if n:
            errors.append(f'audit field {col} has {n} null value(s)')

    # 6. record_hash non-empty
    blank_hash = df['record_hash'].isna() | (df['record_hash'].astype('string').str.len() < 32)
    if int(blank_hash.sum()):
        errors.append(f'record_hash missing or too short: {int(blank_hash.sum())} row(s)')

    # 7. monetary-identity consistency (rounded to 2dp, matching transform)
    g_calc = (df['quantity'].astype('int64') * df['unit_price']).round(2)
    d_calc = (g_calc * df['discount_pct']).round(2)
    n_calc = (g_calc - d_calc).round(2)
    for name, calc, stored in [
        ('gross_amount',    g_calc, df['gross_amount']),
        ('discount_amount', d_calc, df['discount_amount']),
        ('net_amount',      n_calc, df['net_amount']),
    ]:
        diff = (calc - stored).abs() > 0.01  # tolerance for float rounding
        if int(diff.sum()):
            errors.append(f'{name} inconsistent with formula: {int(diff.sum())} row(s)')

    return errors
