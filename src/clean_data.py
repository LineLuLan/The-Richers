"""
clean_data.py
=============
Clean 13 file CSV thô → output ra data/interim/.

Mọi rule có comment "[từ profiling cell X]" để truy vết.

Cách dùng:
    python src/clean_data.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

# =========================================================================
# CONFIG
# =========================================================================
RAW_DIR = Path('data/raw')
INTERIM_DIR = Path('data/interim')
INTERIM_DIR.mkdir(parents=True, exist_ok=True)

REPORT: Dict[str, dict] = {}


def _log(table: str, action: str, n_before: int, n_after: int, note: str = ''):
    REPORT.setdefault(table, {})[action] = {
        'n_before': int(n_before),
        'n_after': int(n_after),
        'n_dropped': int(n_before - n_after),
        'note': note,
    }


# =========================================================================
# LOAD
# =========================================================================
def load_raw() -> Dict[str, pd.DataFrame]:
    """Load 13 file CSV với parse_dates đã chỉ định."""
    print('🔄 Loading raw CSVs...')

    tables = {}
    tables['products'] = pd.read_csv(RAW_DIR / 'products.csv')
    tables['customers'] = pd.read_csv(
        RAW_DIR / 'customers.csv', parse_dates=['signup_date']
    )
    tables['promotions'] = pd.read_csv(
        RAW_DIR / 'promotions.csv', parse_dates=['start_date', 'end_date']
    )
    tables['geography'] = pd.read_csv(RAW_DIR / 'geography.csv')
    tables['orders'] = pd.read_csv(
        RAW_DIR / 'orders.csv', parse_dates=['order_date']
    )
    tables['order_items'] = pd.read_csv(
        RAW_DIR / 'order_items.csv',
        dtype={'promo_id': 'object', 'promo_id_2': 'object'},
    )
    tables['payments'] = pd.read_csv(RAW_DIR / 'payments.csv')
    tables['shipments'] = pd.read_csv(
        RAW_DIR / 'shipments.csv', parse_dates=['ship_date', 'delivery_date']
    )
    tables['returns'] = pd.read_csv(
        RAW_DIR / 'returns.csv', parse_dates=['return_date']
    )
    tables['reviews'] = pd.read_csv(
        RAW_DIR / 'reviews.csv', parse_dates=['review_date']
    )
    tables['sales_train'] = pd.read_csv(
        RAW_DIR / 'sales.csv', parse_dates=['Date']
    )
    tables['inventory'] = pd.read_csv(
        RAW_DIR / 'inventory.csv', parse_dates=['snapshot_date']
    )
    tables['web_traffic'] = pd.read_csv(
        RAW_DIR / 'web_traffic.csv', parse_dates=['date']
    )

    for name, df in tables.items():
        _log(name, '00_loaded', len(df), len(df), f'shape={df.shape}')
        print(f'  ✓ {name:15s} shape={df.shape}')

    return tables


# =========================================================================
# CLEAN FUNCTIONS
# =========================================================================
def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    [Profile cell 8]  product_id unique ✓
    [Profile cell 12] cogs >= price = 0 rows
    [Profile cell 6]  price > 0, cogs > 0
    [Profile cell 4]  size có đúng {S, M, L, XL}
    """
    df = df.copy()
    df['gross_margin'] = (df['price'] - df['cogs']) / df['price']
    _log('products', '01_add_gross_margin', len(df), len(df),
         'gross_margin = (price-cogs)/price')
    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    [Profile cell 8] customer_id unique ✓
    [Profile cell 3] 0 missing → KHÔNG fill 'Unknown'
    """
    return df.copy()


def clean_promotions(df: pd.DataFrame) -> pd.DataFrame:
    """
    promotions: parse dates đã làm ở load. Tính sẵn promo_duration_days.

    [Profile cell 8]  promo_id unique ✓
    [Profile cell 12] 0 promo có start > end
    [Profile cell 3]  applicable_category 80% null → user chọn GIỮ NaN
                       (NaN = áp dụng cho tất cả category)
    [Profile cell 4]  promo_type có 2 values: percentage (45), fixed (5)
    """
    n0 = len(df)
    df = df.copy()

    # Tính sẵn duration để Mart 5 (promotion_perf) dùng
    df['promo_duration_days'] = (df['end_date'] - df['start_date']).dt.days
    _log('promotions', '01_add_duration', n0, len(df),
         'promo_duration_days = end_date - start_date')

    return df


def clean_geography(df: pd.DataFrame) -> pd.DataFrame:
    """[Profile cell 8] zip unique ✓, 0 missing, 0 duplicate."""
    return df.copy()


def clean_orders(df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    [Profile cell 8]  order_id unique ✓
    [Profile cell 9]  0 FK orphans
    [Profile cell 12] 477,453/646,945 (~74%) order_date < signup_date
                       → User chọn: FLAG, không drop
    [Profile cell 4]  order_status có 6 values: delivered, cancelled, returned,
                       shipped, paid, created
    """
    n0 = len(df)
    df = df.copy()

    # FLAG order_date < signup_date
    signup_map = customers_df.set_index('customer_id')['signup_date']
    df['_signup'] = df['customer_id'].map(signup_map)
    df['flag_order_before_signup'] = (df['order_date'] < df['_signup']).astype(int)
    n_flag = df['flag_order_before_signup'].sum()
    df = df.drop(columns=['_signup'])
    _log('orders', '01_flag_order_before_signup', n0, len(df),
         f'{n_flag:,} đơn (~{n_flag/len(df)*100:.1f}%) flagged')

    # is_revenue_eligible: status có nghĩa khách đã/đang trả tiền
    revenue_eligible = ['delivered', 'returned', 'shipped', 'paid']
    df['is_revenue_eligible'] = df['order_status'].isin(revenue_eligible).astype(int)
    n_eligible = df['is_revenue_eligible'].sum()
    _log('orders', '02_add_revenue_eligible', len(df), len(df),
         f'{n_eligible:,} đơn revenue-eligible')

    return df


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    [Profile cell 8]  Composite key (order_id, product_id) — không dedup PK
    [Profile cell 3]  promo_id_2 99.97% null → DROP
    [Profile cell 6]  quantity, unit_price, discount_amount đều >= 0
    """
    n0 = len(df)
    df = df.copy()

    if 'promo_id_2' in df.columns:
        df = df.drop(columns=['promo_id_2'])
        _log('order_items', '01_drop_promo_id_2', n0, len(df),
             '99.97% null + chỉ 2 unique values → drop')

    # line_revenue = qty*price - discount
    df['line_revenue'] = df['quantity'] * df['unit_price'] - df['discount_amount']

    # discount_pct = discount / gross_amount (để DA phân tích depth of discount)
    gross = df['quantity'] * df['unit_price']
    df['discount_pct'] = np.where(gross > 0, df['discount_amount'] / gross, 0)

    _log('order_items', '02_add_derived', len(df), len(df),
         'line_revenue, discount_pct')

    return df


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """[Profile cell 10] orders ↔ payments 1:1 strict ✓."""
    return df.copy()


def clean_shipments(df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    [Profile cell 10] orders ↔ shipments 1:1
    [Profile cell 12] 0 ship > delivery
    [Profile cell 13] 564 đơn status mâu thuẫn shipment → flag tại Mart 2
    """
    df = df.copy()
    df['delivery_lead_time'] = (df['delivery_date'] - df['ship_date']).dt.days
    _log('shipments', '01_add_lead_time', len(df), len(df),
         'delivery_lead_time')
    return df


def clean_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    [Profile cell 8]  return_id unique ✓
    [Profile cell 8]  order_id 1:N (3,877 dups) → agg trước khi join
    [Profile cell 4]  return_reason có 5 values
    """
    return df.copy()


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """[Profile cell 8] review_id unique ✓, rating 1-5 OK."""
    return df.copy()


def clean_sales_train(df: pd.DataFrame) -> pd.DataFrame:
    """
    [Profile cell 8] Date unique = 3,833
    [Profile cell 7] Range 2012-07-04 → 2022-12-31
    """
    return df.copy().sort_values('Date').reset_index(drop=True)


def clean_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """
    [Profile cell 1] Drop product_name/category/segment redundant
                      (đã có trong products.parquet)
    [Profile cell 7] snapshot_date có 126 distinct → đúng monthly
    """
    n0 = len(df)
    df = df.copy()
    redundant = ['product_name', 'category', 'segment']
    drop_cols = [c for c in redundant if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
        _log('inventory', '01_drop_redundant', n0, len(df),
             f'drop {drop_cols}')
    return df


def clean_web_traffic(df: pd.DataFrame) -> pd.DataFrame:
    """[Profile cell 1+7] 1 day = 1 row, KHÔNG composite key."""
    is_unique = df['date'].is_unique
    _log('web_traffic', '01_verify_grain', len(df), len(df),
         f'unique per date: {is_unique}')
    return df.copy()


# =========================================================================
# MAIN PIPELINE
# =========================================================================
def clean_all(save: bool = True) -> Dict[str, pd.DataFrame]:
    """Clean toàn bộ 13 bảng."""
    print('=' * 60)
    print('🧼 CLEANING PIPELINE')
    print('=' * 60)

    raw = load_raw()
    cleaned: Dict[str, pd.DataFrame] = {}

    print('\n[1/3] Master tables...')
    cleaned['products'] = clean_products(raw['products'])
    cleaned['customers'] = clean_customers(raw['customers'])
    cleaned['promotions'] = clean_promotions(raw['promotions'])
    cleaned['geography'] = clean_geography(raw['geography'])

    print('\n[2/3] Transaction tables...')
    cleaned['orders'] = clean_orders(raw['orders'], cleaned['customers'])
    cleaned['order_items'] = clean_order_items(raw['order_items'])
    cleaned['payments'] = clean_payments(raw['payments'])
    cleaned['shipments'] = clean_shipments(raw['shipments'], cleaned['orders'])
    cleaned['returns'] = clean_returns(raw['returns'])
    cleaned['reviews'] = clean_reviews(raw['reviews'])

    print('\n[3/3] Analytical & operational...')
    cleaned['sales_train'] = clean_sales_train(raw['sales_train'])
    cleaned['inventory'] = clean_inventory(raw['inventory'])
    cleaned['web_traffic'] = clean_web_traffic(raw['web_traffic'])

    if save:
        print('\n💾 Saving to data/interim/ as parquet (zstd)...')
        for name, df in cleaned.items():
            out_path = INTERIM_DIR / f'{name}.parquet'
            df.to_parquet(out_path, index=False, compression='zstd')
            print(f'  ✓ {name:15s} → {len(df):>8,} rows')

        report_path = INTERIM_DIR / 'clean_report.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(REPORT, f, indent=2, ensure_ascii=False)
        print(f'\n📋 Clean report → {report_path}')

    print('\n' + '=' * 60)
    print('✅ CLEANING DONE')
    print('=' * 60)

    return cleaned


if __name__ == '__main__':
    clean_all()
