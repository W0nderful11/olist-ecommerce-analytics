# Olist E‑Commerce Analytics

I’m a data analyst at Olist. This repo builds a PostgreSQL database from the public Olist dataset, validates structures/relations, runs baseline checks, and executes 10+ analytical queries, printing results to the terminal (with optional CSV export).

## Project files 
- `scripts/database_setup.py` — create DB `olist_analytics` and schema `olist`.
- `import_olist.py` — full schema reset, CSV load, and foreign keys.
- `database/schema_overview.sql` — quick column types overview.
- `database/relations_check.sql` — relationship checks (anti‑JOIN) and JOIN volumes.
- `database/checks.sql` — baseline checks (LIMIT, WHERE/ORDER, GROUP BY, JOIN).
- `database/queries.sql` — 11 analytical queries.
- `main.py` — universal SQL runner, prints to terminal and optional CSV export.
- `analytics.py` — 6 charts (2+ JOIN each), Excel export with formatting, Plotly time slider; default run builds all.
- `demo.py` — short SQL‑only demo: insert one order_item for seller 'demo', then delete.
- `charts/` — saved charts (images).
- `exports/` — Excel exports.
- `out/` — Plotly HTMLs and other outputs.

## ERD screenshots
First (v2) and core flows used in the course deliverable:

![ERD v2](screen/erd2.jpg)

![ERD core](screen/erd.jpg)

## How to run (short)
1) Import and validate DB
```bash
# 1) Create DB and schema (one‑time)
python3 scripts/database_setup.py

# 2) Full reset + load CSVs from ./data (Kaggle Olist)
python3 import_olist.py --host localhost --port 5432 --dbname olist_analytics \
  --user postgres --password postgres --data-dir ./data

# 3) Validate structures and relations
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d olist_analytics -f database/schema_overview.sql
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d olist_analytics -f database/relations_check.sql
```

2) Baseline checks (LIMIT, WHERE/ORDER, GROUP BY, JOIN)
```bash
python3 main.py --dbname olist_analytics --user postgres --password postgres --sql-file database/checks.sql
```

3) 10+ analytics
```bash
# print
python3 main.py
```

Install dependencies (once):
```bash
pip install -r requirements.txt
```

All charts are powered by SQL with 2+ JOINs, and the script prints a short report for each: rows count, chart type, and what it shows.

Default run (no flags) now builds everything at once: 6 static charts, Excel export, and opens the interactive time slider.
```bash
python3 analytics.py
```

## Tools & data
- PostgreSQL, Python (psycopg2, tabulate)
- Dataset: Brazilian E‑Commerce Public Dataset by Olist (Kaggle)

## GitHub repo
`https://github.com/W0nderful11/olist-ecommerce-analytics.git`
