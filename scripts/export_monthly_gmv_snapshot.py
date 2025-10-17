#!/usr/bin/env python3
"""
Export Monthly GMV Snapshot (Assignment #3 — Task 10 helper)

Creates a static CSV snapshot of the live monthly GMV view so that in Superset
you can import this CSV as a separate (static) data source and then build a
comparison line chart: LIVE (DB) vs SNAPSHOT (CSV).

Steps to use:
 1. Ensure views are created (run database/superset_assignment3.sql).
 2. Run this script once (or whenever you want to refresh static baseline):
       python scripts/export_monthly_gmv_snapshot.py
 3. Import the generated CSV into Superset (Data > Upload a CSV) with name like
       v_monthly_gmv_snapshot
 4. Build a chart that merges LIVE (SQLA) + SNAPSHOT (CSV) via "Dataset" union
    or using Superset's combined dataset feature (or separate charts for demo).

CLI Args mirror PG envs; default path: exports/superset/monthly_gmv_snapshot.csv
"""
from __future__ import annotations
import os, sys, argparse, csv
from datetime import datetime

try:
    import psycopg2
except Exception:
    print('Requires psycopg2-binary. Install: pip install psycopg2-binary', file=sys.stderr)
    raise


def parse_args():
    p = argparse.ArgumentParser(description='Export static snapshot of monthly GMV view')
    p.add_argument('--host', default=os.environ.get('PGHOST','localhost'))
    p.add_argument('--port', default=os.environ.get('PGPORT','5432'))
    p.add_argument('--dbname', default=os.environ.get('PGDATABASE','olist_analytics'))
    p.add_argument('--user', default=os.environ.get('PGUSER','postgres'))
    p.add_argument('--password', default=os.environ.get('PGPASSWORD','postgres'))
    p.add_argument('--out', default='exports/superset/monthly_gmv_snapshot.csv')
    return p.parse_args()


def main():
    args = parse_args()
    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname, user=args.user, password=args.password)
    cur = conn.cursor()
    cur.execute("SELECT month, gmv FROM olist.v_monthly_gmv_live ORDER BY month;")
    rows = cur.fetchall()
    out_path = args.out
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['month','gmv'])
        for r in rows:
            w.writerow([r[0].strftime('%Y-%m-%d'), r[1]])
    print(f'Snapshot saved: {out_path} ({len(rows)} rows)')
    cur.close(); conn.close()

if __name__ == '__main__':
    main()
