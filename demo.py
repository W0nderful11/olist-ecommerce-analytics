#!/usr/bin/env python3
"""
1) Ensure demo seller exists (seller_id='demo').
2) Single INSERT (WITH CTE) computes the needed amount and inserts one order_item for 'demo'.
3) After you re-generate charts in analytics.py, DELETE that inserted item to revert DB.
"""
import argparse
import os
import sys
import warnings

from analytics import DBParams, connect_db, build_sqlalchemy_engine


def _ensure_demo_seller(conn) -> str:
    """Гарантируем наличие продавца с seller_id='demo' и возвращаем его id."""
    cur = conn.cursor()
    demo_id = "demo"
    cur.execute("SELECT 1 FROM olist.sellers WHERE seller_id=%s;", (demo_id,))
    if cur.fetchone() is None:
        cur.execute(
            """
            INSERT INTO olist.sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
            VALUES (%s, 00000, 'demo_city', 'BA')
            ON CONFLICT (seller_id) DO NOTHING;
            """,
            (demo_id,),
        )
        conn.commit()
    return demo_id


def _insert_demo_item_cte(conn, seller_id: str):
    """One SQL (WITH CTE): compute needed amount so 'demo' becomes #1 and insert a single order_item.
    Returns (order_id, order_item_id).
    """
    cur = conn.cursor()
    cur.execute(
        """
        WITH top1 AS (
            SELECT SUM(oi.price) AS revenue
            FROM olist.order_items oi
            JOIN olist.sellers s ON s.seller_id = oi.seller_id
            JOIN olist.orders  o ON o.order_id = oi.order_id
            GROUP BY s.seller_id
            ORDER BY revenue DESC
            LIMIT 1
        ), demo_rev AS (
            SELECT COALESCE(SUM(price),0) AS revenue
            FROM olist.order_items
            WHERE seller_id = %(seller)s
        ), need AS (
            SELECT GREATEST(100.0, t.revenue * 0.01) + (t.revenue - d.revenue) AS amount
            FROM top1 t, demo_rev d
        ), any_product AS (
            SELECT product_id FROM olist.products LIMIT 1
        ), rnd_order AS (
            SELECT order_id FROM olist.orders ORDER BY random() LIMIT 1
        ), next_item AS (
            SELECT rn.order_id, COALESCE(MAX(oi.order_item_id),0)+1 AS item_id
            FROM rnd_order rn
            LEFT JOIN olist.order_items oi ON oi.order_id = rn.order_id
            GROUP BY rn.order_id
        ), ins AS (
            INSERT INTO olist.order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
            SELECT ni.order_id, ni.item_id, ap.product_id, %(seller)s, NOW() + INTERVAL '7 day', n.amount, 0.00
            FROM next_item ni, any_product ap, need n
            RETURNING order_id, order_item_id
        )
        SELECT order_id, order_item_id FROM ins;
        """,
        {"seller": seller_id},
    )
    order_id, order_item_id = cur.fetchone()
    conn.commit()
    return order_id, order_item_id


def _delete_demo_item(conn, order_id, order_item_id):
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM olist.order_items WHERE order_id = %s AND order_item_id = %s;",
        (order_id, order_item_id),
    )
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Olist — демо для защиты")
    parser.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    parser.add_argument("--port", default=os.environ.get("PGPORT", "5432"))
    parser.add_argument("--dbname", default=os.environ.get("PGDATABASE", "olist_analytics"))
    parser.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    parser.add_argument("--password", default=os.environ.get("PGPASSWORD", "postgres"))
    args = parser.parse_args()

    dbp = DBParams(args.host, args.port, args.dbname, args.user, args.password)
    print("Подключение к PostgreSQL:", {k: getattr(dbp, k) for k in ("host", "port", "dbname", "user")})

    try:
        # тише в консоли
        warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable", category=UserWarning)
        build_sqlalchemy_engine(dbp)
        with connect_db(dbp) as conn:
            # 1) Ensure demo seller exists
            demo_seller_id = _ensure_demo_seller(conn)

            # 2) One CTE INSERT — compute amount and insert row
            order_id, order_item_id = _insert_demo_item_cte(conn, demo_seller_id)
            print(f"Inserted DEMO order_item: order={order_id}, item_id={order_item_id}, seller={demo_seller_id}")
            print("Now re-run charts to show the effect:")
            print("  python3 analytics.py")
            input("Press ENTER to delete the demo row and revert DB… ")

            # 3) Delete the inserted row
            _delete_demo_item(conn, order_id, order_item_id)
            print("Demo row removed. DB reverted.")
    except Exception as e:
        print("Ошибка демо:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
