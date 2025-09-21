#!/usr/bin/env python3
"""
Aster E‑Commerce Analytics (Olist) — основной скрипт
Запускает набор SQL‑запросов против базы PostgreSQL `olist_analytics`.
Выполняет 10+ аналитических запросов по заказам, товарам, оплатам и отзывам.
"""

import argparse
import csv
import sys
import time
from decimal import Decimal

try:
    import psycopg2
    import psycopg2.extras
except Exception as e:
    print("Missing dependency 'psycopg2'. Install with: pip install psycopg2-binary", file=sys.stderr)
    raise

try:
    from tabulate import tabulate
    HAVE_TABULATE = True
except Exception:
    HAVE_TABULATE = False

def load_queries_from_file(path: str):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            sql_text = f.read()
        # Split by ';' while keeping statements that look like SELECT
        parts = [p.strip() for p in sql_text.split(';')]
        queries = []
        idx = 0
        for p in parts:
            if not p:
                continue
            # only run SELECT statements
            if 'select' in p.lower():
                idx += 1
                name = f"q{idx:02d}"
                queries.append((name, p + ';'))
        return queries
    except Exception as e:
        print(f"Failed to load queries from {path}: {e}")
        return []


def normalize_value(v):
    """Convert Decimal and other non-serializable types to str for printing/csv."""
    if isinstance(v, Decimal):
        return str(v)
    if v is None:
        return ""
    return v


def print_table(title, columns, rows):
    print("\n" + "=" * 80)
    print(title)
    print("-" * 80)
    if not rows:
        print("(no rows)")
        return
    headers = columns
    printable_rows = [[normalize_value(col) for col in row] for row in rows]
    if HAVE_TABULATE:
        print(tabulate(printable_rows, headers=headers, tablefmt="grid", stralign="left", numalign="right"))
    else:
        # simple fallback
        col_widths = [max(len(str(h)), max((len(str(r[i])) for r in printable_rows), default=0)) for i, h in enumerate(headers)]
        fmt = " | ".join("{:%d}" % w for w in col_widths)
        print(fmt.format(*headers))
        print("-" * (sum(col_widths) + 3 * (len(col_widths)-1)))
        for r in printable_rows:
            print(fmt.format(*r))


def save_csv(path, columns, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for row in rows:
            w.writerow([normalize_value(col) for col in row])


def run_all(conn, args):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    sql_path = args.sql_file if getattr(args, 'sql_file', '') else 'database/queries.sql'
    queries = load_queries_from_file(sql_path)
    for key, sql in queries:
        print(f"\nRunning [{key}] ...")
        start = time.time()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed = time.time() - start
            # convert RealDict rows to list of tuples to preserve column order
            columns = list(rows[0].keys()) if rows else []
            row_tuples = [tuple(r[col] for col in columns) for r in rows] if rows else []
            print(f"Query finished in {elapsed:.3f}s — {len(row_tuples)} rows")
            print_table(f"{key} — {sql.splitlines()[0]}", columns, row_tuples)
            if args.save_csv:
                fname = f"{key}.csv" if not args.csv_dir else f"{args.csv_dir.rstrip('/')}/{key}.csv"
                save_csv(fname, columns, row_tuples)
                print(f"Saved CSV -> {fname}")
        except Exception as e:
            print(f"Error running query [{key}]: {e}", file=sys.stderr)
            # don't stop: continue to next query
    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Запуск аналитических запросов Olist и вывод/сохранение результатов.")
    parser.add_argument("--host", default="localhost", help="DB host (default: localhost)")
    parser.add_argument("--port", default="5432", help="DB port (default: 5432)")
    parser.add_argument("--dbname", default="olist_analytics", help="Database name")
    parser.add_argument("--user", default="postgres", help="DB user")
    parser.add_argument("--password", default="postgres", help="DB password")
    parser.add_argument("--save-csv", dest="save_csv", action="store_true", help="Save each result to CSV files")
    parser.add_argument("--csv-dir", dest="csv_dir", default="", help="Directory to save CSVs to (default: current dir)")
    parser.add_argument("--timeout", type=int, default=60, help="Statement timeout in seconds")
    parser.add_argument("--sql-file", dest="sql_file", default="", help="Путь к SQL-файлу (по умолчанию database/queries.sql)")
    args = parser.parse_args()

    conn_info = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
    }
    if args.user:
        conn_info["user"] = args.user
    if args.password:
        conn_info["password"] = args.password

    print("🛒 Aster E‑Commerce Analytics — Olist")
    print("=" * 60)
    print("Connecting to PostgreSQL with:", {k: conn_info[k] for k in ("host", "port", "dbname", "user")})
    try:
        conn = psycopg2.connect(**conn_info)
        # set statement timeout (ms)
        cur = conn.cursor()
        cur.execute("SET statement_timeout = %s;", (args.timeout * 1000,))
        conn.commit()
        cur.close()

        run_all(conn, args)
        conn.close()
        print("\n✅ Все запросы Olist выполнены успешно!")
    except Exception as e:
        print("❌ Connection or execution failed:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()