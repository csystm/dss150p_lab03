"""Unit tests for the pipeline's transformation rules.

These test the internal `_stage_*` helpers and the public `build_curated`,
`record_hash`, and `validate_curated` functions directly — no Docker, no
PostgreSQL, no Airflow required. Run with:

    pytest -q
"""
import pandas as pd

from src.common.audit import record_hash
from src.transform.staging import (
    _dedup_latest,
    _stage_customers,
    _stage_orders,
    _stage_products,
)
from src.transform.curated import build_curated
from src.validate.quality import validate_curated


RUN_ID = 'run_test'
STAGED_AT = '2026-01-01T00:00:00+00:00'


#  _dedup_latest

def test_dedup_keeps_latest_updated_at():
    df = pd.DataFrame({
        'customer_id': ['C1', 'C1', 'C2'],
        'value':       ['old', 'new', 'only'],
        'updated_at':  pd.to_datetime([
            '2025-01-01T00:00:00Z',
            '2025-02-01T00:00:00Z',
            '2025-01-01T00:00:00Z',
        ], utc=True),
    })
    out = _dedup_latest(df, 'customer_id', 'updated_at')
    assert len(out) == 2
    c1 = out.loc[out['customer_id'] == 'C1'].iloc[0]
    assert c1['value'] == 'new'


def test_dedup_tiebreak_is_last_row():
    """Equal updated_at: the last row in file order wins (stable, deterministic)."""
    df = pd.DataFrame({
        'customer_id': ['C1', 'C1'],
        'value':       ['first', 'second'],
        'updated_at':  pd.to_datetime([
            '2025-01-01T00:00:00Z',
            '2025-01-01T00:00:00Z',
        ], utc=True),
    })
    out = _dedup_latest(df, 'customer_id', 'updated_at')
    assert len(out) == 1
    assert out.iloc[0]['value'] == 'second'


# customers staging

def _customers_raw():
    return pd.DataFrame({
        'customer_id':   ['C1', 'C2'],
        'first_name':    ['Alice', 'Bob'],
        'last_name':     ['A', 'B'],
        'email':         ['Alice@Example.COM', None],
        'city':          ['  manila  ', 'Cebu'],
        'customer_tier': ['Gold', 'Silver'],
        'created_at':    ['2024-01-01T00:00:00+00:00', '2024-01-01T00:00:00+00:00'],
        'updated_at':    ['2024-02-01T00:00:00+00:00', '2024-02-01T00:00:00+00:00'],
    })


def test_customers_normalizes_email_and_city():
    valid, quarantine = _stage_customers(_customers_raw(), RUN_ID, STAGED_AT)
    row = valid.loc[valid['customer_id'] == 'C1'].iloc[0]
    assert row['email'] == 'alice@example.com'
    assert row['city'] == 'Manila'


def test_customers_quarantines_missing_email():
    valid, quarantine = _stage_customers(_customers_raw(), RUN_ID, STAGED_AT)
    assert 'C2' not in valid['customer_id'].tolist()
    assert len(quarantine) == 1
    q = quarantine.iloc[0]
    assert q['business_key'] == 'C2'
    assert q['reason'] == 'missing_email'


# products staging

def _products_raw():
    return pd.DataFrame({
        'product_id': ['P1', 'P2'],
        'name':       ['Widget', 'Bad'],
        'category': [
            {'name': 'Tools', 'department': 'Hardware'},
            {'name': 'Misc',  'department': 'Other'},
        ],
        'brand':       ['W', 'B'],
        'unit_price':  [10.0, -5.0],
        'active':      [True, True],
        'updated_at':  ['2024-01-01T00:00:00+00:00', '2024-01-01T00:00:00+00:00'],
    })


def test_products_flattens_category():
    valid, _ = _stage_products(_products_raw(), RUN_ID, STAGED_AT)
    row = valid.loc[valid['product_id'] == 'P1'].iloc[0]
    assert row['category'] == 'Tools'
    assert row['department'] == 'Hardware'


def test_products_quarantines_invalid_price():
    valid, quarantine = _stage_products(_products_raw(), RUN_ID, STAGED_AT)
    assert 'P2' not in valid['product_id'].tolist()
    assert len(quarantine) == 1
    assert quarantine.iloc[0]['reason'] == 'invalid_price'


# orders staging

def _orders_raw():
    return pd.DataFrame({
        'order_id':        ['O1', 'O2', 'O3'],
        'customer_id':     ['C1', 'C1', 'C1'],
        'product_id':      ['P1', 'P1', 'P1'],
        'order_timestamp': ['2025-01-01T00:00:00+00:00'] * 3,
        'quantity':        [5, 0, 5],
        'unit_price':      [10.0, 10.0, 10.0],
        'discount_pct':    [0.0, 0.0, 0.0],
        'status':          ['DELIVERED', 'DELIVERED', 'BOGUS'],
        'updated_at':      ['2025-01-02T00:00:00+00:00'] * 3,
    })


def test_orders_quarantines_invalid_quantity():
    valid, quarantine = _stage_orders(_orders_raw(), RUN_ID, STAGED_AT)
    assert 'O2' not in valid['order_id'].tolist()
    reasons = dict(zip(quarantine['business_key'], quarantine['reason']))
    assert reasons.get('O2') == 'invalid_quantity'


def test_orders_quarantines_invalid_status():
    valid, quarantine = _stage_orders(_orders_raw(), RUN_ID, STAGED_AT)
    assert 'O3' not in valid['order_id'].tolist()
    reasons = dict(zip(quarantine['business_key'], quarantine['reason']))
    assert reasons.get('O3') == 'invalid_status'


# record_hash

def test_record_hash_is_deterministic():
    r = {'order_id': 'O1', 'quantity': 5, 'status': 'DELIVERED'}
    assert record_hash(r, list(r.keys())) == record_hash(r, list(r.keys()))


def test_record_hash_ignores_columns_not_in_key_list():
    """Audit columns excluded from the hash must not affect it."""
    keys = ['order_id', 'quantity']
    h1 = record_hash({'order_id': 'O1', 'quantity': 5, 'pipeline_run_id': 'r1'}, keys)
    h2 = record_hash({'order_id': 'O1', 'quantity': 5, 'pipeline_run_id': 'r2'}, keys)
    assert h1 == h2


# build_curated

def _staging_for_curated():
    customers = pd.DataFrame({
        'customer_id':   ['C1'],
        'city':          ['Manila'],
        'customer_tier': ['Gold'],
    })
    products = pd.DataFrame({
        'product_id': ['P1'],
        'name':       ['Widget'],
        'category':   ['Tools'],
        'brand':      ['W'],
    })
    orders = pd.DataFrame({
        'order_id':        ['O1', 'O2'],
        'customer_id':     ['C1', 'C_MISSING'],
        'product_id':      ['P1', 'P1'],
        'order_timestamp': pd.to_datetime(['2025-01-01T00:00:00Z'] * 2, utc=True),
        'quantity':        [5, 2],
        'unit_price':      [10.0, 10.0],
        'discount_pct':    [0.1, 0.0],
        'status':          ['DELIVERED', 'DELIVERED'],
        'updated_at':      pd.to_datetime(['2025-01-02T00:00:00Z'] * 2, utc=True),
        'pipeline_run_id': [RUN_ID, RUN_ID],
        'staged_at_utc':   [STAGED_AT, STAGED_AT],
    })
    return {'customers': customers, 'products': products, 'orders': orders}


def test_build_curated_computes_amounts():
    staging = _staging_for_curated()
    curated, _ = build_curated(staging, RUN_ID)
    row = curated.iloc[0]
    # 5 * 10.00 = 50.00 gross; 10% discount = 5.00; net = 45.00
    assert row['gross_amount'] == 50.00
    assert row['discount_amount'] == 5.00
    assert row['net_amount'] == 45.00


def test_build_curated_quarantines_orphan_customer():
    staging = _staging_for_curated()
    curated, orphan_q = build_curated(staging, RUN_ID)
    assert 'O2' not in curated['order_id'].tolist()
    assert len(orphan_q) == 1
    assert orphan_q.iloc[0]['business_key'] == 'O2'
    assert orphan_q.iloc[0]['reason'] == 'orphan_customer'


# ---------- validate_curated -------------------------------------------------

def _curated_min():
    return pd.DataFrame({
        'order_id':         ['O1', 'O2'],
        'customer_id':      ['C1', 'C1'],
        'product_id':       ['P1', 'P1'],
        'quantity':         [5, 5],
        'unit_price':       [10.0, 10.0],
        'discount_pct':     [0.0, 0.0],
        'gross_amount':     [50.0, 50.0],
        'discount_amount':  [0.0, 0.0],
        'net_amount':       [50.0, 50.0],
        'status':           ['DELIVERED', 'DELIVERED'],
        'pipeline_run_id':  [RUN_ID, RUN_ID],
        'processed_at_utc': [STAGED_AT, STAGED_AT],
        'record_hash':      ['a' * 64, 'b' * 64],
        'source_updated_at':[STAGED_AT, STAGED_AT],
    })


def test_validate_passes_clean_dataframe():
    assert validate_curated(_curated_min()) == []


def test_validate_detects_duplicate_order_id():
    df = _curated_min()
    df.loc[1, 'order_id'] = 'O1'  # duplicate
    errors = validate_curated(df)
    assert any('duplicate' in e.lower() for e in errors)


def test_validate_detects_negative_net_amount():
    df = _curated_min()
    df.loc[0, 'net_amount'] = -1.0
    errors = validate_curated(df)
    assert any('net_amount' in e and 'negative' in e for e in errors)


def test_validate_detects_disallowed_status():
    df = _curated_min()
    df.loc[0, 'status'] = 'BOGUS'
    errors = validate_curated(df)
    assert any('status' in e for e in errors)
