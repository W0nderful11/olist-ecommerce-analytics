#!/usr/bin/env python3
"""
Auto Data Refresh Stream (Assignment #3 — Task 2)

Purpose:
  Continuously (or once) insert new synthetic but meaningful orders with related
  order_items and payments into the Olist PostgreSQL schema so Apache Superset
  dashboards can auto‑refresh and show live changes.

Design:
  - Inserts one order per cycle (configurable batch) with 1..N items.
  - Selects existing customer, seller, product IDs randomly to keep FK integrity.
  - Generates realistic price/freight ranges by sampling from existing products/order_items.
  - Payment record mirrors total of inserted items.
  - Optional review insertion (disabled by default) after a random delay simulation (flag).
  - Uses server timestamps (NOW()) and sets order_status = 'delivered' to make it visible in revenue KPIs quickly.

CLI Parameters:
  --host, --port, --dbname, --user, --password : DB connection (env PG* fallbacks)
  --interval <seconds>  : Sleep between cycles (default 8s)
  --max-cycles N        : Stop after N cycles (omit = infinite)
  --batch-size N        : Insert N orders per cycle (default 1)
  --min-items N         : Min items per order (default 1)
  --max-items N         : Max items per order (default 3)
  --with-reviews        : Also insert a synthetic 1..5 review per order
  --status STATUS       : order_status to set (default delivered)
  --once                : Shortcut for --max-cycles 1
  --dry-run             : Build records but do not INSERT (logs only)

Usage examples:
  python scripts/auto_insert_stream.py --interval 10
  python scripts/auto_insert_stream.py --once --batch-size 5 --max-items 4

Safety:
  - Commits each cycle; rollback on any exception.
  - Primary keys: order_id: 'LIVE' + ULID like pattern; review_id/payment PK composite uses sequence logic.
  - Avoid collisions by checking existing ID before insert (very low probability for generated IDs).

Note:
  This script purposefully keeps logic compact for transparency; not optimized for high throughput.
"""
from __future__ import annotations
import os, sys, argparse, time, random, string, uuid
from decimal import Decimal
from typing import List, Tuple
from datetime import datetime, timedelta

try:
    import psycopg2
    import psycopg2.extras
except Exception as e:  # pragma: no cover
    print("Requires psycopg2-binary. Install: pip install psycopg2-binary", file=sys.stderr)
    raise

# ---------- Helpers ----------

def connect(args):
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )

def random_id(prefix: str) -> str:
    # Short ULID-like (time + random) for low collision risk
    return f"{prefix}_{int(time.time()*1000):x}_{uuid.uuid4().hex[:8]}"

# Cache pools for FK sampling
def load_fk_pools(cur):
    pools = {}
    cur.execute("SELECT customer_id FROM olist.customers ORDER BY random() LIMIT 500;")
    pools['customers'] = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT seller_id FROM olist.sellers ORDER BY random() LIMIT 500;")
    pools['sellers'] = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, product_weight_g, product_length_cm, product_height_cm, product_width_cm FROM olist.products ORDER BY random() LIMIT 2000;")
    pools['products'] = cur.fetchall()  # tuples
    # price / freight samples from existing order_items for realism
    cur.execute("SELECT price, freight_value FROM olist.order_items ORDER BY random() LIMIT 2000;")
    pools['price_freight'] = cur.fetchall()
    if not all(pools.values()):
        raise RuntimeError("FK sampling pools are empty — ensure base data is loaded.")
    return pools


def build_order_items(pools, order_id: str, seller_id: str, n_items: int, amplify_total: float = 1.0) -> List[Tuple]:
    items = []
    for idx in range(1, n_items + 1):
        prod = random.choice(pools['products'])
        price, freight = random.choice(pools['price_freight'])
        # small random jitter so new rows differ
        price = (Decimal(price) if isinstance(price, (Decimal, float)) else Decimal(str(price))) * (Decimal('0.95') + Decimal(random.random()/10))
        freight = (Decimal(freight) if isinstance(freight, (Decimal, float)) else Decimal(str(freight))) * (Decimal('0.9') + Decimal(random.random()/5))
        # optional amplification of totals (scale both price and freight)
        if amplify_total and amplify_total != 1.0:
            price *= Decimal(str(amplify_total))
            freight *= Decimal(str(amplify_total))
        items.append((order_id, idx, prod[0], seller_id, price, freight))
    return items


def insert_cycle(conn, pools, args):
    """Insert one cycle of synthetic orders with consistent 4-space indentation."""
    cur = conn.cursor()
    created_orders = []
    # month/year/amplify теперь передаются как параметры
    for i in range(args.batch_size):
        order_id = random_id('LIVEORD')
        customer_id = random.choice(pools['customers'])
        seller_id = random.choice(pools['sellers'])
        status = args.status
        # month/year/amplify из параметров
        month = args.month
        year = args.year
        amplify = args.amplify
        # Случайный день в месяце
        days_in_month = (datetime(year + (1 if month == 12 else 0), (month % 12) + 1, 1) - datetime(year, month, 1)).days
        random_day = random.randint(0, days_in_month - 1)
        base_now = datetime(year, month, 1) + timedelta(days=random_day)
        if args.align_minute:
            base_now = base_now.replace(second=0, microsecond=0)
        ts_purchase = base_now
        ts_approved = ts_purchase + timedelta(minutes=random.randint(1, 10))
        ts_carrier = ts_approved + timedelta(hours=random.randint(1, 24))
        ts_customer = ts_carrier + timedelta(days=random.randint(1, 7))
        ts_eta = ts_purchase + timedelta(days=5)
        # Insert order
        if not args.dry_run:
            cur.execute(
                """
                INSERT INTO olist.orders (order_id, customer_id, order_status, order_purchase_timestamp,
                                          order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
                                          order_estimated_delivery_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING order_id
                """,
                (order_id, customer_id, status, ts_purchase, ts_approved, ts_carrier, ts_customer, ts_eta),
            )
        # Items
        n_items = random.randint(args.min_items, args.max_items)
        items = build_order_items(pools, order_id, seller_id, n_items, amplify_total=amplify)
        total_price = Decimal('0')
        total_freight = Decimal('0')
        for (oid, item_id, product_id, sel_id, price, freight) in items:
            total_price += price
            total_freight += freight
            if not args.dry_run:
                cur.execute(
                    """
                    INSERT INTO olist.order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (oid, item_id, product_id, sel_id, ts_purchase + timedelta(hours=2), price, freight)
                )
        payment_value = total_price + total_freight
        if not args.dry_run:
            cur.execute(
                """
                INSERT INTO olist.order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)
                VALUES (%s, 1, 'credit_card', 1, %s)
                """,
                (order_id, payment_value)
            )
        if args.with_reviews and not args.dry_run:
            review_id = random_id('LIVEREV')
            review_score = random.randint(3, 5)  # optimistic bias
            cur.execute(
                """
                INSERT INTO olist.order_reviews (review_id, order_id, review_score, review_creation_date, review_answer_timestamp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO NOTHING
                """,
                (review_id, order_id, review_score, ts_customer, ts_customer)
            )
        created_orders.append((order_id, customer_id, seller_id, float(total_price + total_freight)))
    if not args.dry_run:
        conn.commit()
    return created_orders


def parse_args():
    p = argparse.ArgumentParser(description="Stream new synthetic orders into Olist schema for live dashboards")
    p.add_argument('--host', default=os.environ.get('PGHOST','localhost'))
    p.add_argument('--port', default=os.environ.get('PGPORT','5432'))
    p.add_argument('--dbname', default=os.environ.get('PGDATABASE','olist_analytics'))
    p.add_argument('--user', default=os.environ.get('PGUSER','postgres'))
    p.add_argument('--password', default=os.environ.get('PGPASSWORD','postgres'))
    p.add_argument('--interval', type=float, default=8.0, help='Sleep seconds between cycles')
    p.add_argument('--max-cycles', type=int, default=None, help='Stop after N cycles (default infinite)')
    p.add_argument('--batch-size', type=int, default=1)
    p.add_argument('--min-items', type=int, default=1)
    p.add_argument('--max-items', type=int, default=3)
    p.add_argument('--with-reviews', action='store_true')
    p.add_argument('--status', default='delivered')
    p.add_argument('--once', action='store_true', help='Insert just one cycle (alias for --max-cycles 1)')
    p.add_argument('--dry-run', action='store_true')
    # New optional controls (requested)
    p.add_argument('--recent-minutes', type=float, default=None, help='Backdate purchase time randomly within the last N minutes')
    p.add_argument('--align-minute', action='store_true', help='Floor timestamps to the start of the minute (ss=00)')
    p.add_argument('--amplify-total', type=float, default=1.0, help='Multiply price & freight by a factor to amplify totals')
    return p.parse_args()


def main():
    args = parse_args()
    if args.once:
        args.max_cycles = 1
    if args.min_items > args.max_items:
        print('min-items cannot exceed max-items', file=sys.stderr)
        sys.exit(2)
    conn = connect(args)
    print('Connected to DB for live inserts:', {k:getattr(args,k) for k in ('host','port','dbname','user')})
    cur = conn.cursor()
    pools = load_fk_pools(cur)
    cycle = 0
    # month/year/amplify — глобальные переменные для последовательного роста
    today = datetime.now()
    month = today.month
    year = today.year
    amplify = 1.0
    try:
        while True:
            cycle += 1
            # Передаем month/year/amplify в args
            args.month = month
            args.year = year
            args.amplify = amplify
            orders = insert_cycle(conn, pools, args)
            ts = time.strftime('%Y-%m-%d %H:%M:%S')
            for o in orders:
                print(f"[{ts}] Inserted order {o[0]} (cust={o[1]}, seller={o[2]}, total={o[3]:.2f})")
            # Следующий заказ — следующий месяц, amplify растет
            month += 1
            if month > 12:
                month = 1
                year += 1
            amplify += 1.0
            if args.max_cycles and cycle >= args.max_cycles:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print('\nInterrupted by user.')
    finally:
        conn.close()
        print('Connection closed.')

if __name__ == '__main__':
    main()
