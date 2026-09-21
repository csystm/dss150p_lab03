"""Curated layer: join valid orders to customers/products, compute monetary
measures, and emit audit columns. Orphan references are quarantined, never
silently dropped.
"""
from __future__ import annotations

import pandas as pd

from src.common.audit import utc_now_iso, record_hash
from src.transform.staging import (
    _build_quarantine,
    _empty_quarantine,
)


# Matches the column order of curated.sales_order_lines in sql/init/01_warehouse_schema.sql
CURATED_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'customer_city', 'customer_tier',
    'product_name', 'category', 'brand',
    'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount',
    'status',
    'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
]

# Deterministic business-content hash: audit/volatile columns are excluded.
RECORD_HASH_KEYS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'customer_city', 'customer_tier',
    'product_name', 'category', 'brand',
    'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount',
    'status', 'source_updated_at',
]


def build_curated(staging: dict, run_id: str):
    """Join valid orders to customers/products and compute monetary measures.

    Returns:
        (curated_df, orphan_quarantine_df)

    Orphan orders (missing customer or product in valid staging) are placed in
    orphan_quarantine_df with reason `orphan_customer`, `orphan_product`, or
    `orphan_customer_and_product`. They are never silently dropped.
    """
    orders    = staging['orders'].copy()
    customers = staging['customers'].copy()
    products  = staging['products'].copy()

    processed_at = utc_now_iso()

    valid_cust_ids = set(customers['customer_id'].dropna().tolist())
    valid_prod_ids = set(products['product_id'].dropna().tolist())

    orphan_cust_mask = ~orders['customer_id'].isin(valid_cust_ids)
    orphan_prod_mask = ~orders['product_id'].isin(valid_prod_ids)
    orphan_mask = orphan_cust_mask | orphan_prod_mask

    # orphan quarantine
    orphan_rows = orders.loc[orphan_mask].copy()
    if len(orphan_rows) > 0:
        reason = pd.Series(['orphan'] * len(orphan_rows),
                           index=orphan_rows.index, dtype='object')
        ob_cust = ~orphan_rows['customer_id'].isin(valid_cust_ids)
        ob_prod = ~orphan_rows['product_id'].isin(valid_prod_ids)
        reason = reason.mask(ob_cust & ob_prod,  'orphan_customer_and_product')
        reason = reason.mask(ob_cust & ~ob_prod, 'orphan_customer')
        reason = reason.mask(~ob_cust & ob_prod, 'orphan_product')
        orphan_q = _build_quarantine(
            source='orders',
            key_col='order_id',
            rows=orphan_rows,
            reasons=reason,
            drop_cols=[],
            run_id=run_id,
            staged_at=processed_at,
        )
    else:
        orphan_q = _empty_quarantine()

    # curated from valid rows
    valid_orders = orders.loc[~orphan_mask].copy()

    cust_view = (customers[['customer_id', 'city', 'customer_tier']]
                 .rename(columns={'city': 'customer_city'}))
    prod_view = (products[['product_id', 'name', 'category', 'brand']]
                 .rename(columns={'name': 'product_name'}))

    merged = (valid_orders
              .merge(cust_view, on='customer_id', how='left', validate='many_to_one')
              .merge(prod_view, on='product_id',  how='left', validate='many_to_one'))

    # monetary measures (rounded to 2dp for NUMERIC(_,2) parity)
    merged['quantity']     = merged['quantity'].astype('int64')
    merged['unit_price']   = merged['unit_price'].astype('float64').round(2)
    merged['discount_pct'] = merged['discount_pct'].astype('float64')

    merged['gross_amount']    = (merged['quantity'] * merged['unit_price']).round(2)
    merged['discount_amount'] = (merged['gross_amount'] * merged['discount_pct']).round(2)
    merged['net_amount']      = (merged['gross_amount'] - merged['discount_amount']).round(2)

    # audit columns
    merged['source_updated_at'] = merged['updated_at']
    merged['pipeline_run_id']   = run_id
    merged['processed_at_utc']  = processed_at

    curated = merged[CURATED_COLUMNS[:-1]].copy()  # all but record_hash
    curated['record_hash'] = curated.apply(
        lambda r: record_hash(r.to_dict(), RECORD_HASH_KEYS),
        axis=1,
    )
    curated = curated[CURATED_COLUMNS].reset_index(drop=True)

    return curated, orphan_q
