"""
build_marts.py
==============
Build 5 Data Marts từ data/interim/ → data/processed/.

5 Marts (cover 13/13 bảng raw):
    Mart 1: order_lines      (~715k rows)  — 1 dòng = 1 orderline (full enrich)
    Mart 2: orders           (~647k rows)  — 1 dòng = 1 order
    Mart 3: products         (2,412 rows)  — 1 dòng = 1 product
    Mart 4: daily_ops        (~4,400 rows) — 1 dòng = 1 day (gộp date_dim)
    Mart 5: promotion_perf   (50 rows)     — 1 dòng = 1 promo

Quy tắc sinh tử: aggregate TRƯỚC khi join (tránh nhân bản dòng do quan hệ 1:N).

[Mọi rule có comment "[từ profile X]" để truy vết.]
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

INTERIM_DIR = Path('data/interim')
PROCESSED_DIR = Path('data/processed')
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

PARQUET_OPTS = dict(index=False, compression='zstd')


def load_interim() -> Dict[str, pd.DataFrame]:
    """Load 13 bảng đã clean."""
    print('🔄 Loading cleaned tables...')
    tables = {}
    for name in ['products', 'customers', 'promotions', 'geography', 'orders',
                 'order_items', 'payments', 'shipments', 'returns', 'reviews',
                 'sales_train', 'inventory', 'web_traffic']:
        tables[name] = pd.read_parquet(INTERIM_DIR / f'{name}.parquet')
        print(f'  ✓ {name:15s} shape={tables[name].shape}')
    return tables


# =========================================================================
# MART 1: ORDER LINES (FULL ENRICH) ⭐ NEW
# =========================================================================
def build_order_lines_mart(t: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Mart 1: 1 row = 1 orderline (= 1 dòng trong order_items).

    Dùng cho EDA chéo nhiều chiều (region × segment × promo × age...).
    KHÔNG có aggregation — đây là grain thấp nhất, DA tự groupby khi cần.

    Pipeline:
        order_items (center, ~715k rows)
        ↓ left join products (qua product_id, N:1)
        ↓ left join orders (qua order_id, N:1)
        ↓ left join customers (qua customer_id, N:1)
        ↓ left join geography (qua zip, N:1)
        ↓ left join promotions (qua promo_id, N:1)

    Cardinality verify: PHẢI giữ nguyên 715k rows sau khi join.
    [Từ profile cell 1] order_items = 714,669 rows.
    """
    print('\n📦 Building MART 1: order_lines (full enrich)...')

    # Bắt đầu từ order_items
    mart = t['order_items'].copy()
    n_initial = len(mart)
    print(f'   • Start: {n_initial:,} orderlines')

    # JOIN 1: products (N:1)
    # [Từ profile cell 9] 0 FK orphans → an toàn left join
    products_cols = ['product_id', 'product_name', 'category', 'segment',
                     'size', 'color', 'price', 'cogs', 'gross_margin']
    mart = mart.merge(t['products'][products_cols], on='product_id', how='left')
    print(f'   • + products       → {len(mart):,} rows')

    # JOIN 2: orders (N:1) — mỗi orderline thuộc 1 order
    orders_cols = ['order_id', 'order_date', 'customer_id', 'zip',
                   'order_status', 'payment_method', 'device_type',
                   'order_source', 'flag_order_before_signup',
                   'is_revenue_eligible']
    mart = mart.merge(t['orders'][orders_cols], on='order_id', how='left')
    print(f'   • + orders         → {len(mart):,} rows')

    # JOIN 3: customers (N:1)
    cust_cols = ['customer_id', 'gender', 'age_group', 'acquisition_channel',
                 'signup_date']
    mart = mart.merge(t['customers'][cust_cols], on='customer_id', how='left')
    print(f'   • + customers      → {len(mart):,} rows')

    # JOIN 4: geography (N:1) qua zip
    geo_cols = ['zip', 'region', 'district']
    mart = mart.merge(t['geography'][geo_cols], on='zip', how='left')
    print(f'   • + geography      → {len(mart):,} rows')

    # JOIN 5: promotions (N:1) qua promo_id
    # [Từ profile cell 3] 61% rows promo_id null → many will have NaN promotion fields
    promo_cols = ['promo_id', 'promo_name', 'promo_type', 'discount_value',
                  'applicable_category', 'promo_channel', 'stackable_flag',
                  'min_order_value']
    mart = mart.merge(t['promotions'][promo_cols], on='promo_id', how='left')
    print(f'   • + promotions     → {len(mart):,} rows')

    # SANITY CHECK: row count phải giữ nguyên!
    if len(mart) != n_initial:
        raise RuntimeError(
            f'Row count đã thay đổi! Bắt đầu {n_initial}, '
            f'sau join còn {len(mart)}. Có quan hệ 1:N gây nhân bản!'
        )
    print(f'   ✓ Row count preserved: {len(mart):,}')

    # Time features (DA dùng để filter theo period)
    mart['order_year'] = mart['order_date'].dt.year
    mart['order_month'] = mart['order_date'].dt.month
    mart['order_quarter'] = mart['order_date'].dt.quarter
    mart['order_dow'] = mart['order_date'].dt.dayofweek

    print(f'   ✓ MART 1 final shape: {mart.shape}')
    return mart


# =========================================================================
# MART 2: ORDERS (1 row = 1 order)
# =========================================================================
def build_orders_mart(t: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Mart 2: 1 row = 1 order. Customer behavior view.

    [Từ profile cell 8] orders.order_id unique = 646,945 rows.
    [Từ profile cell 10] orders↔payments 1:1, orders↔shipments 1:0/1
    [Từ profile cell 8]  orders↔returns 1:N → AGG TRƯỚC khi join
    [Từ profile cell 8]  orders↔reviews 1:N → AGG TRƯỚC khi join
    """
    print('\n📦 Building MART 2: orders (customer behavior)...')

    # Step 1: agg order_items về order level
    oi = t['order_items']
    oi_agg = oi.groupby('order_id').agg(
        n_items=('product_id', 'count'),
        n_unique_products=('product_id', 'nunique'),
        total_quantity=('quantity', 'sum'),
        order_revenue=('line_revenue', 'sum'),
        total_discount=('discount_amount', 'sum'),
        has_promo=('promo_id', lambda s: int(s.notna().any())),
    ).reset_index()
    print(f'   • order_items agg → {len(oi_agg):,} orders')

    # Step 2: agg returns về order level (1:N → 1)
    ret = t['returns']
    ret_agg = ret.groupby('order_id').agg(
        n_returns=('return_id', 'count'),
        total_return_qty=('return_quantity', 'sum'),
        total_refund=('refund_amount', 'sum'),
    ).reset_index()
    print(f'   • returns agg     → {len(ret_agg):,} orders có return')

    # Step 3: agg reviews về order level (1:N → 1)
    rv = t['reviews']
    rv_agg = rv.groupby('order_id').agg(
        n_reviews=('review_id', 'count'),
        avg_rating=('rating', 'mean'),
    ).reset_index()
    print(f'   • reviews agg     → {len(rv_agg):,} orders có review')

    # Step 4: bắt đầu từ orders, left join all
    mart = t['orders'].copy()

    # order_items aggregated
    mart = mart.merge(oi_agg, on='order_id', how='left')

    # payments (1:1) — verified
    pay_cols = ['order_id', 'payment_value', 'installments']
    mart = mart.merge(t['payments'][pay_cols], on='order_id', how='left')

    # customers (N:1)
    cust_cols = ['customer_id', 'gender', 'age_group', 'acquisition_channel',
                 'signup_date', 'city']
    cust = t['customers'][cust_cols].rename(columns={'city': 'customer_city'})
    mart = mart.merge(cust, on='customer_id', how='left')

    # geography (N:1) qua zip giao hàng
    geo = t['geography'][['zip', 'region', 'district']].rename(
        columns={'district': 'shipping_district'}
    )
    mart = mart.merge(geo, on='zip', how='left')

    # shipments (1:0 hoặc 1:1)
    ship_cols = ['order_id', 'ship_date', 'delivery_date', 'shipping_fee',
                 'delivery_lead_time']
    mart = mart.merge(t['shipments'][ship_cols], on='order_id', how='left')

    # returns aggregated
    mart = mart.merge(ret_agg, on='order_id', how='left')
    # reviews aggregated
    mart = mart.merge(rv_agg, on='order_id', how='left')

    # Cờ phụ trợ
    mart['has_shipment'] = mart['ship_date'].notna().astype(int)
    mart['is_returned'] = (mart['order_status'] == 'returned').astype(int)
    mart['is_cancelled'] = (mart['order_status'] == 'cancelled').astype(int)
    mart['is_reviewed'] = mart['n_reviews'].fillna(0).gt(0).astype(int)

    # [Từ profile cell 13] FLAG status mâu thuẫn shipment (564 đơn)
    expected_to_have_shipment = ['delivered', 'returned', 'shipped']
    mart['flag_status_shipment_mismatch'] = (
        mart['order_status'].isin(expected_to_have_shipment) &
        (mart['has_shipment'] == 0)
    ).astype(int)
    n_mm = mart['flag_status_shipment_mismatch'].sum()
    print(f'   • flag_status_shipment_mismatch: {n_mm:,} đơn')

    # customer tenure
    mart['customer_tenure_days'] = (
        mart['order_date'] - mart['signup_date']
    ).dt.days

    # Fill 0 cho count metrics
    fill_zero = ['n_items', 'n_unique_products', 'total_quantity',
                 'order_revenue', 'total_discount', 'has_promo',
                 'n_returns', 'total_return_qty', 'total_refund', 'n_reviews']
    for col in fill_zero:
        if col in mart.columns:
            mart[col] = mart[col].fillna(0)

    # Time features
    mart['order_year'] = mart['order_date'].dt.year
    mart['order_month'] = mart['order_date'].dt.month
    mart['order_quarter'] = mart['order_date'].dt.quarter
    mart['order_dow'] = mart['order_date'].dt.dayofweek

    # SANITY: phải bằng số order ban đầu
    n_orders = t['orders']['order_id'].nunique()
    if len(mart) != n_orders:
        raise RuntimeError(f'Mart 2 row count: {len(mart)} ≠ orders {n_orders}')
    print(f'   ✓ MART 2 final shape: {mart.shape}')
    return mart


# =========================================================================
# MART 3: PRODUCTS (1 row = 1 product)
# =========================================================================
def build_products_mart(t: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Mart 3: 1 row = 1 product. Product diagnostics view.

    [Từ profile cell 1] products = 2,412 rows
    [Từ profile cell 8] inventory↔products 1:N → AGG trước
    """
    print('\n📦 Building MART 3: products (diagnostics)...')

    # Sales agg (từ order_items)
    oi = t['order_items']
    sales_agg = oi.groupby('product_id').agg(
        total_units_sold=('quantity', 'sum'),
        total_orders_with_product=('order_id', 'nunique'),
        total_revenue=('line_revenue', 'sum'),
        total_discount_given=('discount_amount', 'sum'),
        n_orderlines=('product_id', 'count'),
        avg_unit_price=('unit_price', 'mean'),
        avg_discount_pct=('discount_pct', 'mean'),
    ).reset_index()

    # Returns agg
    ret = t['returns']
    ret_agg = ret.groupby('product_id').agg(
        total_units_returned=('return_quantity', 'sum'),
        total_refund=('refund_amount', 'sum'),
        n_return_records=('return_id', 'count'),
    ).reset_index()

    top_reason = (
        ret.groupby('product_id')['return_reason']
        .agg(lambda s: s.mode().iat[0] if len(s) > 0 else None)
        .rename('top_return_reason')
        .reset_index()
    )
    ret_agg = ret_agg.merge(top_reason, on='product_id', how='left')

    # Reviews agg
    rv = t['reviews']
    rv_agg = rv.groupby('product_id').agg(
        n_reviews=('review_id', 'count'),
        avg_rating=('rating', 'mean'),
        pct_5_star=('rating', lambda s: (s == 5).mean()),
        pct_1_2_star=('rating', lambda s: (s <= 2).mean()),
    ).reset_index()

    # Inventory agg (lifetime)
    inv = t['inventory']
    inv_agg = inv.groupby('product_id').agg(
        avg_stock_on_hand=('stock_on_hand', 'mean'),
        total_stockout_days=('stockout_days', 'sum'),
        avg_fill_rate=('fill_rate', 'mean'),
        avg_sell_through=('sell_through_rate', 'mean'),
        n_months_stockout=('stockout_flag', 'sum'),
        n_months_overstock=('overstock_flag', 'sum'),
    ).reset_index()

    # Build từ products (master)
    mart = t['products'].copy()
    mart = mart.merge(sales_agg, on='product_id', how='left')
    mart = mart.merge(ret_agg, on='product_id', how='left')
    mart = mart.merge(rv_agg, on='product_id', how='left')
    mart = mart.merge(inv_agg, on='product_id', how='left')

    # Fill 0 cho count metrics
    fill_zero = ['total_units_sold', 'total_orders_with_product',
                 'total_revenue', 'total_discount_given', 'n_orderlines',
                 'total_units_returned', 'total_refund', 'n_return_records',
                 'n_reviews']
    for col in fill_zero:
        if col in mart.columns:
            mart[col] = mart[col].fillna(0)

    # Derived metrics
    mart['return_rate_qty'] = np.where(
        mart['total_units_sold'] > 0,
        mart['total_units_returned'] / mart['total_units_sold'], 0,
    )
    mart['return_rate_rows'] = np.where(
        mart['n_orderlines'] > 0,
        mart['n_return_records'] / mart['n_orderlines'], 0,
    )
    mart['net_revenue'] = mart['total_revenue'] - mart['total_refund']
    mart['flag_never_sold'] = (mart['total_units_sold'] == 0).astype(int)

    n_products = len(t['products'])
    if len(mart) != n_products:
        raise RuntimeError(f'Mart 3 row count: {len(mart)} ≠ products {n_products}')
    print(f'   ✓ MART 3 final shape: {mart.shape}')
    print(f'   ✓ Products chưa từng bán: {mart["flag_never_sold"].sum():,}')
    return mart


# =========================================================================
# MART 4: DAILY OPS + DATE DIMENSION (gộp)
# =========================================================================
def build_daily_ops_mart(t: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Mart 4: 1 row = 1 day. Backbone 2012-07-04 → 2024-07-01.

    Phục vụ:
    - DA: phân tích xu hướng theo thời gian
    - MLE: forecasting (kèm date_dim VN seasonality)

    [Từ profile cell 7]
        sales:       2012-07-04 → 2022-12-31  (chỉ train)
        web_traffic: 2013-01-01 → 2022-12-31  (start trễ 6 tháng)
        orders:      2012-07-04 → 2022-12-31
        inventory:   2012-07-31 → 2022-12-31  (monthly)
    """
    print('\n📦 Building MART 4: daily_ops (gộp date_dim)...')

    # Backbone: bao trùm cả train + test period (cho forecasting)
    sales = t['sales_train'].rename(columns={'Date': 'date'})
    date_min = sales['date'].min()
    date_max = pd.Timestamp('2024-07-01')
    backbone = pd.DataFrame({'date': pd.date_range(date_min, date_max, freq='D')})
    print(f'   • Backbone: {date_min.date()} → {date_max.date()} '
          f'({len(backbone):,} days)')

    # Sales (train period)
    mart = backbone.merge(sales, on='date', how='left')

    # Web traffic (1 day = 1 row)
    wt = t['web_traffic'].rename(columns={'date': 'date'})
    mart = mart.merge(wt, on='date', how='left')

    # Orders activity
    orders = t['orders']
    oi = t['order_items']
    order_rev = oi.groupby('order_id')['line_revenue'].sum().reset_index()
    daily_orders = orders.merge(order_rev, on='order_id', how='left')
    daily_orders_agg = daily_orders.groupby('order_date').agg(
        n_orders=('order_id', 'count'),
        n_unique_customers=('customer_id', 'nunique'),
        n_cancelled=('order_status', lambda s: (s == 'cancelled').sum()),
        n_delivered=('order_status', lambda s: (s == 'delivered').sum()),
        revenue_from_orders=('line_revenue', 'sum'),
    ).reset_index().rename(columns={'order_date': 'date'})
    mart = mart.merge(daily_orders_agg, on='date', how='left')

    # Inventory: monthly → broadcast daily
    inv = t['inventory']
    inv_monthly = inv.groupby('snapshot_date').agg(
        total_stock_on_hand=('stock_on_hand', 'sum'),
        total_units_received=('units_received', 'sum'),
        total_units_sold_inv=('units_sold', 'sum'),
        avg_stockout_days=('stockout_days', 'mean'),
        pct_products_stockout=('stockout_flag', 'mean'),
        pct_products_overstock=('overstock_flag', 'mean'),
        avg_fill_rate=('fill_rate', 'mean'),
        avg_sell_through=('sell_through_rate', 'mean'),
        n_products_in_stock=('product_id', 'nunique'),
    ).reset_index()
    inv_monthly['year_month'] = inv_monthly['snapshot_date'].dt.to_period('M')
    mart['year_month'] = mart['date'].dt.to_period('M')
    mart = mart.merge(
        inv_monthly.drop(columns=['snapshot_date']),
        on='year_month', how='left',
    )
    mart = mart.drop(columns=['year_month'])

    # Returns daily
    ret = t['returns']
    daily_returns = ret.groupby('return_date').agg(
        n_returns=('return_id', 'count'),
        total_refund_daily=('refund_amount', 'sum'),
    ).reset_index().rename(columns={'return_date': 'date'})
    mart = mart.merge(daily_returns, on='date', how='left')

    # Fill 0
    fill_zero = ['n_orders', 'n_unique_customers', 'n_cancelled', 'n_delivered',
                 'revenue_from_orders', 'n_returns', 'total_refund_daily']
    for col in fill_zero:
        if col in mart.columns:
            mart[col] = mart[col].fillna(0)

    mart['is_train_period'] = (mart['date'] <= '2022-12-31').astype(int)

    # ---- DATE DIMENSION (gộp luôn) ----
    print('   • Adding date dimension features...')
    mart['year'] = mart['date'].dt.year
    mart['quarter'] = mart['date'].dt.quarter
    mart['month'] = mart['date'].dt.month
    mart['day'] = mart['date'].dt.day
    mart['dayofweek'] = mart['date'].dt.dayofweek
    mart['dayofyear'] = mart['date'].dt.dayofyear
    mart['weekofyear'] = mart['date'].dt.isocalendar().week.astype(int)
    mart['is_weekend'] = (mart['dayofweek'] >= 5).astype(int)
    mart['is_month_start'] = mart['date'].dt.is_month_start.astype(int)
    mart['is_month_end'] = mart['date'].dt.is_month_end.astype(int)
    mart['is_quarter_start'] = mart['date'].dt.is_quarter_start.astype(int)
    mart['is_quarter_end'] = mart['date'].dt.is_quarter_end.astype(int)

    # VN mega sales
    mart['is_1111'] = ((mart['month'] == 11) & (mart['day'] == 11)).astype(int)
    mart['is_1212'] = ((mart['month'] == 12) & (mart['day'] == 12)).astype(int)
    mart['is_99'] = ((mart['month'] == 9) & (mart['day'] == 9)).astype(int)
    mart['is_88'] = ((mart['month'] == 8) & (mart['day'] == 8)).astype(int)
    mart['is_77'] = ((mart['month'] == 7) & (mart['day'] == 7)).astype(int)

    # Black Friday
    def _black_friday(year):
        nov1 = pd.Timestamp(year, 11, 1)
        thursdays = pd.date_range(nov1, nov1 + pd.Timedelta(days=30), freq='W-THU')
        return thursdays[3] + pd.Timedelta(days=1)

    bf_dates = {y: _black_friday(y) for y in range(int(mart['year'].min()),
                                                    int(mart['year'].max()) + 1)}
    mart['is_black_friday'] = mart['date'].isin(bf_dates.values()).astype(int)

    # Tết Nguyên Đán (hard-code từ Wikipedia)
    TET_DATES = {
        2012: '2012-01-23', 2013: '2013-02-10', 2014: '2014-01-31',
        2015: '2015-02-19', 2016: '2016-02-08', 2017: '2017-01-28',
        2018: '2018-02-16', 2019: '2019-02-05', 2020: '2020-01-25',
        2021: '2021-02-12', 2022: '2022-02-01', 2023: '2023-01-22',
        2024: '2024-02-10', 2025: '2025-01-29',
    }
    tet_dates = pd.to_datetime(list(TET_DATES.values()))
    mart['is_tet'] = mart['date'].isin(tet_dates).astype(int)

    def _days_to_next_tet(d):
        future = tet_dates[tet_dates >= d]
        return (future[0] - d).days if len(future) > 0 else np.nan

    mart['days_to_tet'] = mart['date'].apply(_days_to_next_tet)
    mart['is_pre_tet_2w'] = ((mart['days_to_tet'] >= 0) &
                              (mart['days_to_tet'] <= 14)).astype(int)
    mart['is_pre_tet_1w'] = ((mart['days_to_tet'] >= 0) &
                              (mart['days_to_tet'] <= 7)).astype(int)

    # Days to mega sales
    def _days_to_event(date, month, day):
        target = pd.Timestamp(date.year, month, day)
        if date <= target:
            return (target - date).days
        return (pd.Timestamp(date.year + 1, month, day) - date).days

    mart['days_to_1111'] = mart['date'].apply(lambda d: _days_to_event(d, 11, 11))
    mart['days_to_1212'] = mart['date'].apply(lambda d: _days_to_event(d, 12, 12))

    # VN holidays
    mart['is_new_year'] = ((mart['month'] == 1) & (mart['day'] == 1)).astype(int)
    mart['is_reunification'] = ((mart['month'] == 4) & (mart['day'] == 30)).astype(int)
    mart['is_labor_day'] = ((mart['month'] == 5) & (mart['day'] == 1)).astype(int)
    mart['is_national_day'] = ((mart['month'] == 9) & (mart['day'] == 2)).astype(int)

    print(f'   ✓ MART 4 final shape: {mart.shape}')
    return mart


# =========================================================================
# MART 5: PROMOTION PERFORMANCE ⭐ NEW
# =========================================================================
def build_promotion_perf_mart(t: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Mart 5: 1 row = 1 promotion. Promo ROI view.

    [Từ profile cell 1] promotions = 50 rows
    [Từ profile cell 4] order_items.promo_id = 50 unique values + NaN

    Pipeline:
        order_items (chỉ rows có promo_id != null)
        ↓ groupby promo_id, agg metrics
        ↓ left join promotions (50 rows master)
    """
    print('\n📦 Building MART 5: promotion_performance...')

    oi = t['order_items']
    oi_with_promo = oi[oi['promo_id'].notna()].copy()
    print(f'   • Orderlines có promo: {len(oi_with_promo):,} '
          f'({len(oi_with_promo)/len(oi)*100:.1f}% of all orderlines)')

    # Aggregate metrics theo promo
    promo_metrics = oi_with_promo.groupby('promo_id').agg(
        n_orderlines_applied=('promo_id', 'count'),
        n_unique_orders=('order_id', 'nunique'),
        n_unique_products=('product_id', 'nunique'),
        total_quantity_sold=('quantity', 'sum'),
        total_revenue_with_promo=('line_revenue', 'sum'),
        total_discount_given=('discount_amount', 'sum'),
        avg_discount_pct=('discount_pct', 'mean'),
    ).reset_index()

    # Join với master promotions
    mart = t['promotions'].merge(promo_metrics, on='promo_id', how='left')

    # Fill 0 cho promo chưa từng được dùng
    fill_zero = ['n_orderlines_applied', 'n_unique_orders', 'n_unique_products',
                 'total_quantity_sold', 'total_revenue_with_promo',
                 'total_discount_given']
    for col in fill_zero:
        if col in mart.columns:
            mart[col] = mart[col].fillna(0)

    # Derived metrics
    # ROI rough: revenue per dong of discount
    mart['revenue_per_discount_unit'] = np.where(
        mart['total_discount_given'] > 0,
        mart['total_revenue_with_promo'] / mart['total_discount_given'],
        np.nan,
    )

    # Revenue per day of campaign
    mart['revenue_per_day'] = np.where(
        mart['promo_duration_days'] > 0,
        mart['total_revenue_with_promo'] / mart['promo_duration_days'],
        np.nan,
    )

    # Cờ promo chưa từng dùng (orphan promo)
    mart['flag_unused_promo'] = (mart['n_orderlines_applied'] == 0).astype(int)
    n_unused = mart['flag_unused_promo'].sum()

    print(f'   • Promo chưa từng dùng: {n_unused:,}/{len(mart):,}')
    print(f'   ✓ MART 5 final shape: {mart.shape}')
    return mart


# =========================================================================
# MAIN
# =========================================================================
def build_all_marts(save: bool = True) -> Dict[str, pd.DataFrame]:
    """Build tất cả 5 marts."""
    print('=' * 60)
    print('🏗️  BUILDING 5 DATA MARTS')
    print('=' * 60)

    t = load_interim()

    marts = {}
    marts['mart1_order_lines'] = build_order_lines_mart(t)
    marts['mart2_orders'] = build_orders_mart(t)
    marts['mart3_products'] = build_products_mart(t)
    marts['mart4_daily_ops'] = build_daily_ops_mart(t)
    marts['mart5_promotion_perf'] = build_promotion_perf_mart(t)

    if save:
        print('\n💾 Saving to data/processed/ (parquet zstd)...')
        for name, df in marts.items():
            out_path = PROCESSED_DIR / f'{name}.parquet'
            df.to_parquet(out_path, **PARQUET_OPTS)
            size_mb = out_path.stat().st_size / 1024 / 1024
            print(f'  ✓ {name:30s} → {len(df):>8,} rows, {size_mb:>6.1f} MB')

    print('\n' + '=' * 60)
    print('✅ ALL MARTS BUILT')
    print('=' * 60)
    print('\n📌 Coverage check (13/13 raw tables):')
    print('  Mart 1 (order_lines)  → order_items + products + orders + customers + geography + promotions')
    print('  Mart 2 (orders)       → orders + payments + shipments + customers + geography + agg(order_items/returns/reviews)')
    print('  Mart 3 (products)     → products + agg(order_items/returns/reviews/inventory)')
    print('  Mart 4 (daily_ops)    → sales + web_traffic + agg(orders/returns) + broadcast(inventory)')
    print('  Mart 5 (promo_perf)   → promotions + agg(order_items có dùng)')

    return marts


if __name__ == '__main__':
    build_all_marts()
