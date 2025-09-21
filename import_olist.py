
"""
Загрузка датасета Olist (Kaggle: Brazilian E-Commerce Public Dataset by Olist)
в PostgreSQL в схему `olist`.

Требования:
  - Python 3.8+
  - psycopg2-binary

Пример запуска:
  python import_olist.py \
    --host localhost --port 5432 --dbname postgres \
    --user postgres --password postgres \
    --data-dir /Users/atembek.sh/PycharmProjects/aster_car_sales/data

Скрипт:
  1) создаёт схему `olist` (если нет)
  2) создаёт таблицы с подходящими типами
  3) выполняет COPY из CSV с заголовком
  4) создаёт ключевые индексы
"""

import argparse
import os
import sys
from typing import Dict

try:
    import psycopg2
except Exception as exc:  # pragma: no cover
    print("Требуется psycopg2-binary. Установите: pip install psycopg2-binary", file=sys.stderr)
    raise


def connect_db(params: Dict[str, str]):
    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )
    conn.autocommit = False
    return conn


def reset_schema(cur):
    # удалить возможные старые таблицы/схемы, чтобы не плодить дубликаты
    cur.execute("""
        DROP SCHEMA IF EXISTS olist CASCADE;
    """)
    cur.execute("CREATE SCHEMA olist;")


def cleanup_legacy_public_tables(cur):
    # убрать старые копии таблиц в схеме public (если остались от чужих скриптов)
    cur.execute(
        """
        DO $$
        DECLARE t TEXT;
        BEGIN
          FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename IN (
            'customers','orders','order_items','order_payments','order_reviews','products','sellers','geolocation','product_category_name_translation'
          ) LOOP
            EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE;', t);
          END LOOP;
          -- дополнительно снести любые вариации product_category*_translation в public
          FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'product_category%translation%'
          LOOP
            EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE;', t);
          END LOOP;
        END$$;
        """
    )


def create_tables(cur):
    # Определения таблиц (упорядочены с учётом зависимостей)
    # 1) геолокация (много записей на один zip_prefix — FK не навешиваем намеренно)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.geolocation (
            geolocation_zip_code_prefix INTEGER,
            geolocation_lat DOUBLE PRECISION,
            geolocation_lng DOUBLE PRECISION,
            geolocation_city TEXT,
            geolocation_state TEXT
        );
        """
    )

    # 2) перевод категорий (используется для LEFT JOIN, без строгого FK)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.product_category_name_translation (
            product_category_name TEXT PRIMARY KEY,
            product_category_name_english TEXT
        );
        """
    )

    # 3) клиенты и продавцы (без FK к geolocation, т.к. geolocation имеет дубликаты по префиксу)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.customers (
            customer_id TEXT PRIMARY KEY,
            customer_unique_id TEXT,
            customer_zip_code_prefix INTEGER,
            customer_city TEXT,
            customer_state TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.sellers (
            seller_id TEXT PRIMARY KEY,
            seller_zip_code_prefix INTEGER,
            seller_city TEXT,
            seller_state TEXT
        );
        """
    )

    # 4) продукты с FK на перевод категорий
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.products (
            product_id TEXT PRIMARY KEY,
            product_category_name TEXT,
            product_name_lenght INTEGER,
            product_description_lenght INTEGER,
            product_photos_qty INTEGER,
            product_weight_g INTEGER,
            product_length_cm INTEGER,
            product_height_cm INTEGER,
            product_width_cm INTEGER
        );
        """
    )

    # 5) заказы и зависимые
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT REFERENCES olist.customers(customer_id),
            order_status TEXT,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.order_items (
            order_id TEXT REFERENCES olist.orders(order_id),
            order_item_id INTEGER,
            product_id TEXT REFERENCES olist.products(product_id),
            seller_id TEXT REFERENCES olist.sellers(seller_id),
            shipping_limit_date TIMESTAMP,
            price NUMERIC(12,2),
            freight_value NUMERIC(12,2),
            PRIMARY KEY (order_id, order_item_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.order_payments (
            order_id TEXT REFERENCES olist.orders(order_id),
            payment_sequential INTEGER,
            payment_type TEXT,
            payment_installments INTEGER,
            payment_value NUMERIC(12,2),
            PRIMARY KEY (order_id, payment_sequential)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS olist.order_reviews (
            review_id TEXT PRIMARY KEY,
            order_id TEXT REFERENCES olist.orders(order_id),
            review_score INTEGER,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        );
        """
    )


def copy_from_csv(cur, data_dir: str, file_name: str, table_fqn: str, columns: str):
    csv_path = os.path.join(data_dir, file_name)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)
    with open(csv_path, "r", encoding="utf-8") as f:
        # QUOTE по умолчанию '"', поэтому можно не указывать во избежание экранирования
        sql = f"COPY {table_fqn} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');"
        cur.copy_expert(sql=sql, file=f)


def load_all(cur, data_dir: str):
    # Порядок загрузки с учётом FK/зависимостей
    # 1) справочники/ключевые таблицы
    copy_from_csv(
        cur,
        data_dir,
        "olist_customers_dataset.csv",
        "olist.customers",
        ",".join(
            [
                "customer_id",
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state",
            ]
        ),
    )

    # 2) продукты и продавцы (для FK в items)
    copy_from_csv(
        cur,
        data_dir,
        "olist_products_dataset.csv",
        "olist.products",
        ",".join(
            [
                "product_id",
                "product_category_name",
                "product_name_lenght",
                "product_description_lenght",
                "product_photos_qty",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
            ]
        ),
    )

    copy_from_csv(
        cur,
        data_dir,
        "olist_sellers_dataset.csv",
        "olist.sellers",
        ",".join(
            [
                "seller_id",
                "seller_zip_code_prefix",
                "seller_city",
                "seller_state",
            ]
        ),
    )

    # 3) заказы
    copy_from_csv(
        cur,
        data_dir,
        "olist_orders_dataset.csv",
        "olist.orders",
        ",".join(
            [
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ]
        ),
    )

    # order_items через staging c дедупликацией по (order_id, order_item_id)
    cur.execute("DROP TABLE IF EXISTS _stg_order_items;")
    cur.execute(
        """
        CREATE TEMP TABLE _stg_order_items (
            order_id TEXT,
            order_item_id INTEGER,
            product_id TEXT,
            seller_id TEXT,
            shipping_limit_date TIMESTAMP,
            price NUMERIC(12,2),
            freight_value NUMERIC(12,2)
        ) ON COMMIT DROP;
        """
    )
    copy_from_csv(
        cur,
        data_dir,
        "olist_order_items_dataset.csv",
        "_stg_order_items",
        ",".join(
            [
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "shipping_limit_date",
                "price",
                "freight_value",
            ]
        ),
    )
    cur.execute(
        """
        INSERT INTO olist.order_items (
            order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
        )
        SELECT DISTINCT ON (i.order_id, i.order_item_id)
               i.order_id, i.order_item_id, i.product_id, i.seller_id, i.shipping_limit_date, i.price, i.freight_value
        FROM _stg_order_items i
        JOIN olist.products p ON p.product_id = i.product_id
        JOIN olist.sellers  s ON s.seller_id  = i.seller_id
        ON CONFLICT (order_id, order_item_id) DO NOTHING;
        """
    )

    # order_payments через staging c дедупликацией по (order_id, payment_sequential)
    cur.execute("DROP TABLE IF EXISTS _stg_order_payments;")
    cur.execute(
        """
        CREATE TEMP TABLE _stg_order_payments (
            order_id TEXT,
            payment_sequential INTEGER,
            payment_type TEXT,
            payment_installments INTEGER,
            payment_value NUMERIC(12,2)
        ) ON COMMIT DROP;
        """
    )
    copy_from_csv(
        cur,
        data_dir,
        "olist_order_payments_dataset.csv",
        "_stg_order_payments",
        ",".join(
            [
                "order_id",
                "payment_sequential",
                "payment_type",
                "payment_installments",
                "payment_value",
            ]
        ),
    )
    cur.execute(
        """
        INSERT INTO olist.order_payments (
            order_id, payment_sequential, payment_type, payment_installments, payment_value
        )
        SELECT DISTINCT ON (order_id, payment_sequential)
               order_id, payment_sequential, payment_type, payment_installments, payment_value
        FROM _stg_order_payments
        ON CONFLICT (order_id, payment_sequential) DO NOTHING;
        """
    )

    # COPY с дедупликацией review_id через staging
    cur.execute("DROP TABLE IF EXISTS _stg_reviews;")
    cur.execute(
        """
        CREATE TEMP TABLE _stg_reviews (
            review_id TEXT,
            order_id TEXT,
            review_score INTEGER,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        ) ON COMMIT DROP;
        """
    )
    copy_from_csv(
        cur,
        data_dir,
        "olist_order_reviews_dataset.csv",
        "_stg_reviews",
        ",".join(
            [
                "review_id",
                "order_id",
                "review_score",
                "review_comment_title",
                "review_comment_message",
                "review_creation_date",
                "review_answer_timestamp",
            ]
        ),
    )
    cur.execute(
        """
        INSERT INTO olist.order_reviews AS dst (
            review_id, order_id, review_score, review_comment_title,
            review_comment_message, review_creation_date, review_answer_timestamp
        )
        SELECT DISTINCT ON (review_id)
               review_id, order_id, review_score, review_comment_title,
               review_comment_message, review_creation_date, review_answer_timestamp
        FROM _stg_reviews
        ON CONFLICT (review_id) DO NOTHING;
        """
    )

    copy_from_csv(
        cur,
        data_dir,
        "product_category_name_translation.csv",
        "olist.product_category_name_translation",
        ",".join(["product_category_name", "product_category_name_english"]),
    )

    copy_from_csv(
        cur,
        data_dir,
        "olist_geolocation_dataset.csv",
        "olist.geolocation",
        ",".join(
            [
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng",
                "geolocation_city",
                "geolocation_state",
            ]
        ),
    )


def create_indexes(cur):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON olist.orders(customer_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON olist.order_items(order_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON olist.order_items(product_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_seller_id ON olist.order_items(seller_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_order_id ON olist.order_payments(order_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reviews_order_id ON olist.order_reviews(order_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON olist.products(product_category_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customers_zip ON olist.customers(customer_zip_code_prefix);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sellers_zip ON olist.sellers(seller_zip_code_prefix);")


def validate_duplicates(cur):
    checks = [
        ("olist.customers", "customer_id"),
        ("olist.orders", "order_id"),
        ("olist.products", "product_id"),
        ("olist.sellers", "seller_id"),
    ]
    for table, pkcol in checks:
        cur.execute(f"SELECT COUNT(*) AS c, COUNT(DISTINCT {pkcol}) AS d FROM {table};")
        c, d = cur.fetchone()
        if c != d:
            raise RuntimeError(f"Обнаружены дубликаты в {table} по {pkcol}: total={c}, distinct={d}")


def main():
    parser = argparse.ArgumentParser(description="Импорт CSV Olist в PostgreSQL (схема olist)")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="5432")
    parser.add_argument("--dbname", default="olist_analytics")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="postgres")
    parser.add_argument("--data-dir", required=True, help="Путь к каталогу с CSV Olist")
    args = parser.parse_args()

    conn = connect_db(
        {
            "host": args.host,
            "port": args.port,
            "dbname": args.dbname,
            "user": args.user,
            "password": args.password,
        }
    )
    cur = conn.cursor()
    try:
        # Полный сброс и очистка от возможных дубликатов в public
        cleanup_legacy_public_tables(cur)
        reset_schema(cur)
        create_tables(cur)
        conn.commit()

        load_all(cur, args.data_dir)
        conn.commit()

        create_indexes(cur)
        validate_duplicates(cur)
        conn.commit()
        print("Загрузка Olist завершена успешно.")
    except Exception as exc:  # pragma: no cover
        conn.rollback()
        print("Ошибка при загрузке:", exc, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()


