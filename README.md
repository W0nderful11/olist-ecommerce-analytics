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

## Assignment #3 (Apache Superset) Overview

This repository now contains helper scripts and SQL views to accelerate the 15 tasks of Assignment #3 (Midterm). Below is a checklist showing what is already implemented in code and what must be completed manually inside Superset UI.

### Data & Live Inserts
- [x] Base PostgreSQL schema + data import (`scripts/database_setup.py`, `import_olist.py`).
- [x] Live streaming of new facts (Task 2) — `scripts/auto_insert_stream.py` inserts new orders + items + payments every N seconds (FK‑safe, meaningful values). Start it before presenting dashboards:
  ```bash
  python3 scripts/auto_insert_stream.py --interval 8 --batch-size 1
  # or single batch
  python3 scripts/auto_insert_stream.py --once --batch-size 5
  ```

### SQL Views for Superset (Tasks 5–9, 12–14 foundations)
`database/superset_assignment3.sql` creates reusable views:
- `olist.v_geo_customer_state` – centroids for states (Geo Point/Heatmap) (Task 5)
- `olist.v_heatmap_category_state` – category × state GMV matrix (Task 6 Heatmap)
- `olist.v_sunburst_state_category_seller` – hierarchy state → category → seller (Task 7 Sunburst)
- `olist.v_treemap_category_state` – category → state GMV for Treemap (Task 8)
- `olist.v_wordcloud_category_frequency` – frequencies for Word Cloud (Task 9)
- `olist.v_order_totals_enriched` – includes normalized value + segment label (Tasks 13 Normalization, 14 Categorization) — you can also replicate via Calculated Columns UI to demonstrate both approaches.
- `olist.v_monthly_gmv_live` – dynamic monthly GMV (Task 10 live source)
- `olist.v_monthly_gmv_metrics` – provides `gmv` and `prev_gmv` for Growth metrics (Task 12)

Apply them once after data load:
```bash
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d olist_analytics -f database/superset_assignment3.sql
```

### Static Snapshot for Task 10
Create a CSV snapshot to contrast with the live view:
```bash
python3 scripts/export_monthly_gmv_snapshot.py
```
Uploads to `exports/superset/monthly_gmv_snapshot.csv`.
In Superset: Data > Upload CSV (disallowed for raw tables per spec, but allowed here as engineered duplicate for the comparison requirement). Name dataset e.g. `monthly_gmv_snapshot` and build a 2‑line chart (LIVE vs SNAPSHOT). Show divergence as the streaming script keeps inserting new orders.

### Charts Reproduction (Task 4)
Already produced in `analytics.py` (pie, bar, barh, line, histogram, scatter). Rebuild analogs in Superset using base tables or provided views. Ensure axis labels, titles, legends are business‑friendly (no raw IDs without context).

### Geo Visualization (Task 5)
Use `v_geo_customer_state`. Configure point size = customers, tooltip with state code. Optionally switch to Heatmap layer if density visualization is preferred.

### Heatmap (Task 6)
Dataset: `v_heatmap_category_state`. Rows = category, Columns = state, Metric = SUM(gmv) or COUNT(items). Apply top filters if needed.

### Sunburst (Task 7)
Dataset: `v_sunburst_state_category_seller`. Levels: customer_state > category > seller_id. Metric: SUM(revenue).

### Treemap (Task 8)
Dataset: `v_treemap_category_state`. Hierarchy: category > state. Value: SUM(gmv). Color: same metric or distinct dimension.

### Word Cloud (Task 9)
Dataset: `v_wordcloud_category_frequency`. Word: category; Metric: frequency.

### Metrics (Task 12)
In Superset UI (NOT raw SQL):
1. Average: AVG(gmv) (dataset `v_monthly_gmv_live` or `v_monthly_gmv_metrics`).
2. Median: PERCENTILE(gmv, 0.5).
3. Growth %: (gmv - prev_gmv)/prev_gmv from `v_monthly_gmv_metrics` (set prev_gmv as a metric or use custom formula). Display table with both AVG & MEDIAN to compare.

### Calculated Columns & Categorization (Tasks 13, 14)
Demonstrate UI approach although `v_order_totals_enriched` provides examples:
- Normalization formula (0–1): `(order_total - min(order_total)) / (max(order_total) - min(order_total))` using Superset Calculated Column.
- Categorization: CASE or inline conditions to create segments (LOW/MEDIUM/HIGH/ULTRA) if re‑implementing rather than relying on the view.

### Filters, Drill‑Down, Cross‑Filtering (Task 11)
- Add native dashboard filters: Date range (order purchase), Category, State, Seller.
- Enable cross-filtering in charts (checkbox in each chart’s Interaction settings) so selecting a category or state refines others.
- Drill-down: configure hierarchical charts (sunburst/treemap) and enable “drill to detail” (or set extra grouping levels which appear on click).

### Dashboards Design (Task 3)
Suggested thematic dashboards:
1. Customers – geo, heatmap, repeat purchase metrics, order value distribution.
2. Sellers & Products – sunburst, treemap, word cloud, top sellers bar, monthly GMV line (with live vs snapshot).
Ensure consistent color palette, readable labels (rename raw columns like `customer_state` → `Customer State`).

### Dashboard Export (Task 15)
Export JSON/YAML and commit into `superset_exports/` (this folder already exists with a README placeholder).

### Presentation Flow (Suggested)
1. Start live stream: `python3 scripts/auto_insert_stream.py --interval 8`.
2. Open dashboard with auto-refresh (set refresh interval in Superset to 30s or manual refresh demonstration).
3. Show Live vs Snapshot monthly chart diverging.
4. Interact: cross-filter heatmap, treemap, sunburst.
5. Highlight calculated columns & metrics differences (AVG vs Median, normalized value, segments).
6. Mention export JSON location in repo.

### Remaining Manual Steps (Not in code)
- Actual chart creation and styling in Superset (Tasks 3–9, 11–15 visualization layer).
- Creating metrics & calculated columns via Superset UI (Tasks 12–14) — code gives reference but you must create them through the interface for grading criteria.

### Quick Command Summary
```bash
# Create / reset and load data
python3 scripts/database_setup.py
python3 import_olist.py --data-dir ./data

# Create views for Superset
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d olist_analytics -f database/superset_assignment3.sql

# Start live inserts (keep running)
python3 scripts/auto_insert_stream.py --interval 8

# Create static snapshot (run once before or during demo)
python3 scripts/export_monthly_gmv_snapshot.py
```

---
### Dashboard Screenshots

#### Customers Dashboard
![Customers Dashboard](charts/customers-2025-10-17T05-26-23.110Z.jpg)

#### Sellers & Products Dashboard
![Sellers & Products Dashboard](charts/sellers-products-2025-10-17T05-27-33.703Z.jpg)

All required code-side foundations for Assignment #3 are present. Proceed to build and style the dashboards in Superset and export them into `superset_exports/`.

---

# Assignment #4: Prometheus and Grafana Monitoring Setup

This project sets up a monitoring stack using Prometheus and Grafana with three dashboards: Database Exporter, Node Exporter, and Custom Exporter.

## Prerequisites

- Docker and Docker Compose installed
- PostgreSQL database running (adjust connection in docker-compose.yml)
- OpenWeather API key (sign up at https://openweathermap.org/api)

## Setup Instructions

1. **Clone or navigate to the project directory**

2. **Set up environment variables**
   Create a `.env` file in the root directory:
   ```
   OPENWEATHER_API_KEY=your_api_key_here
   ```

3. **Configure PostgreSQL connection**
   In `docker-compose.yml`, update the `DATA_SOURCE_NAME` for postgres_exporter to match your PostgreSQL setup:
   ```
   DATA_SOURCE_NAME=postgresql://username:password@host:port/database?sslmode=disable
   ```
   For local PostgreSQL, use `host.docker.internal` if on macOS/Windows.

4. **Build and run the monitoring stack**
   ```bash
   docker-compose up --build
   ```

5. **Access the services**
   - Prometheus: http://localhost:9090 
   - Grafana: http://localhost:3000 (admin/admin)
   - PostgreSQL Exporter: http://localhost:9187
   - Node Exporter: http://localhost:9100
   - Custom Exporter: http://localhost:8000

## Dashboards

### Database Exporter Dashboard
Monitors PostgreSQL database metrics.

### Node Exporter Dashboard
Monitors system resources (CPU, memory, disk, network).

### Custom Exporter Dashboard
Collects weather data from OpenWeather API.

## PromQL Queries

### Database Exporter (10 queries)
1. `pg_stat_database_blks_hit / (pg_stat_database_blks_hit + pg_stat_database_blks_read) * 100` - Cache hit ratio
2. `rate(pg_stat_database_tup_fetched[5m])` - Tuple fetch rate
3. `pg_stat_database_numbackends` - Active connections
4. `pg_database_size_bytes / 1024 / 1024 / 1024` - Database size in GB
5. `time() - pg_postmaster_start_time_seconds` - Uptime in seconds
6. `rate(pg_stat_database_xact_commit[5m])` - Transaction commit rate
7. `rate(pg_stat_database_xact_rollback[5m])` - Transaction rollback rate
8. `pg_stat_user_tables_n_tup_ins` - Total inserts
9. `pg_stat_user_tables_n_tup_upd` - Total updates
10. `pg_stat_user_tables_n_tup_del` - Total deletes

### Node Exporter (10 queries)
1. `100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)` - CPU usage %
2. `node_load1` - Load average 1m
3. `node_memory_MemTotal_bytes / 1024 / 1024 / 1024` - Total memory GB
4. `node_memory_MemAvailable_bytes / 1024 / 1024 / 1024` - Available memory GB
5. `node_memory_SwapTotal_bytes / 1024 / 1024 / 1024` - Total swap GB (replacement for disk, as filesystem collector doesn't work on macOS)
6. `rate(node_disk_read_bytes_total[5m])` - Disk read rate bytes/sec
7. `rate(node_network_receive_bytes_total[5m]) * 8 / 1000000` - Network receive Mbit/sec
8. `node_timex_frequency_adjustment_ratio` - Frequency adjustment ratio (replacement for temperature, as hwmon doesn't work on macOS)
9. `node_procs_blocked` - Blocked processes (replacement for battery, as battery collector doesn't work on macOS)
10. `node_procs_running` - Running processes

### Custom Exporter (10 queries)
1. `weather_temperature_celsius` - Current temperature
2. `weather_humidity_percent` - Current humidity
3. `weather_pressure_hpa` - Current pressure
4. `weather_wind_speed_ms` - Wind speed
5. `weather_wind_direction_degrees` - Wind direction
6. `weather_visibility_meters` - Visibility
7. `weather_clouds_percent` - Cloud coverage
8. `weather_rain_1h_mm` - Rain in last hour
9. `weather_snow_1h_mm` - Snow in last hour
10. `rate(weather_api_calls_total[5m])` - API call rate

## Grafana Dashboards

Import the JSON files from the `grafana/dashboards/` directory into Grafana.

## Alerts

Configure alerts in Grafana for each dashboard as per requirements.

## Notes

- Ensure PostgreSQL is accessible from the Docker network
- For macOS, use `host.docker.internal` for host connections
- Metrics collection should run for 1-5 hours as required
- All exporters must show "UP" in Prometheus targets
---


# Задание №4: Настройка мониторинга Prometheus и Grafana

В этом проекте настраивается стек мониторинга с использованием Prometheus и Grafana с тремя информационными панелями: Экспортер базы данных, экспортер узлов и пользовательский экспортер.

## Предварительные требования

- Установлены Docker и Docker Compose
- Запущена база данных PostgreSQL (настройте подключение в docker-compose.yml)
- Ключ API OpenWeather (зарегистрируйтесь по адресу https://openweathermap.org/api)

## Инструкции по настройке

1. **Клонировать или перейти в каталог проекта**

2. **Настроить переменные среды**
   Создайте файл `.env` в корневом каталоге:
   ```
   OPENWEATHER_API_KEY=ваш_api_key_ здесь
   ```

3. **Настройте подключение к PostgreSQL**
   В `docker-compose.yml` обновите `DATA_SOURCE_NAME` для postgres_exporter, чтобы оно соответствовало вашим настройкам PostgreSQL:
   ```
   DATA_SOURCE_NAME=postgresql://username:password@host:port/database?sslmode=disable
   ```
   Для локального PostgreSQL используйте `host.docker.internal`, если он установлен на macOS/Windows.

4. **Создайте и запустите стек мониторинга**
   ``bash
docker-compose up -сборка
   ```

5. **Доступ к сервисам**
   - Прометей: http://localhost:9090 (Prometheus)
   - Графана: http://localhost:3000 (Grafana)
   - Экспортер PostgreSQL: http://localhost:9187  (DB)  - должны быть метрики pg_stat_database_numbackends.
   - Экспортер узлов: http://localhost:9100 (Node) — метрики node_cpu_seconds_total
   - Пользовательский экспортер: http://localhost:8000 (Custom) — метрики  weather_temperature_celsius.

## Информационные панели

### Панель экспортера баз данных
Отслеживает показатели базы данных PostgreSQL.

### Панель экспортера узлов
Отслеживает системные ресурсы (процессор, память, диск, сеть).

### Панель пользовательского экспортера
Собирает данные о погоде с помощью OpenWeather API.

## Запросы PromQL

### Экспорт базы данных (10 запросов)
1. `pg_stat_database_blks_hit / (pg_stat_database_blks_hit + pg_stat_database_blks_read) * 100` - Коэффициент попадания в кэш
2. `rate(pg_stat_database_tup_fetched[5m])` - Скорость выборки кортежей
3. `avg_over_time(pg_stat_database_numbackends[5m])` - Среднее активных соединений за 5 мин
4. `avg_over_time(pg_database_size_bytes[5m]) / 1024 / 1024 / 1024` - Средний размер базы данных в ГБ за 5 мин
5. `up{job="postgres_exporter"}` - Статус экспортера (1=UP, 0=DOWN; используйте вместо uptime, если pg_postmaster_start_time_seconds пустой)
6. `rate(pg_stat_database_xact_commit[5m])` - Скорость фиксации транзакции
7. `avg_over_time(pg_stat_database_xact_rollback[5m])` - Среднее количество откатов за 5 мин (используйте avg_over_time для gauge-метрики)
8. `avg_over_time(pg_stat_user_tables_n_tup_ins[5m])` - Среднее количество вставок за 5 мин (используйте avg_over_time для gauge-метрики)
9. `avg_over_time(pg_stat_user_tables_n_tup_upd[5m])` - Среднее количество обновлений за 5 мин (используйте avg_over_time для gauge-метрики)
10. `avg_over_time(pg_stat_user_tables_n_tup_del[5m])` - Среднее количество удалений за 5 мин (используйте avg_over_time для gauge-метрики)

### Экспортер узлов (10 запросов)
1. `100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)` - Загрузка процессора %
2. `avg_over_time(node_load1[5m])` - Средняя загрузка за 5 мин
3. `avg_over_time(node_memory_MemTotal_bytes[5m]) / 1024 / 1024 / 1024` - Средний общий объем памяти в ГБ за 5 мин
4. `avg_over_time(node_memory_MemAvailable_bytes[5m]) / 1024 / 1024 / 1024` - Средний объем доступной памяти в ГБ за 5 мин
5. `avg_over_time(node_memory_SwapTotal_bytes[5m]) / 1024 / 1024 / 1024` - Средний общий объем swap в ГБ за 5 мин (замена для диска, так как filesystem collector не работает на macOS)
6. `rate(node_disk_read_bytes_total[5m])` - скорость чтения с диска, байт/сек
7. `rate(node_network_receive_bytes_total[5m]) * 8 / 1000000` - скорость приема по сети Мбит/с
8. `avg_over_time(node_timex_frequency_adjustment_ratio[5m])` - Среднее соотношение корректировки частоты за 5 мин (замена для температуры, так как hwmon не работает на macOS)
9. `avg_over_time(node_procs_blocked[5m])` - Среднее количество заблокированных процессов за 5 мин (замена для батареи, так как battery collector не работает на macOS)
10. `avg_over_time(node_procs_running[5m])` - Среднее количество запущенных процессов за 5 мин

### Пользовательский экспортер (10 запросов)
1. `avg_over_time(weather_temperature_celsius[5m])` - Средняя температура за 5 мин
2. `avg_over_time(weather_humidity_percent[5m])` - Средняя влажность за 5 мин
3. `avg_over_time(weather_pressure_hpa[5m])` - Среднее давление за 5 мин
4. `avg_over_time(weather_wind_speed_ms[5m])` - Средняя скорость ветра за 5 мин
5. `avg_over_time(weather_wind_direction_degrees[5m])` - Среднее направление ветра за 5 мин
6. `avg_over_time(weather_visibility_meters[5m])` - Средняя видимость за 5 мин
7. `avg_over_time(weather_clouds_percent[5m])` - Средняя облачность за 5 мин
8. `avg_over_time(weather_rain_1h_mm[5m])` - Средний дождь за 5 мин
9. `avg_over_time(weather_snow_1h_mm[5m])` - Средний снег за 5 мин
10. `rate(weather_api_calls_total[5m])` - частота вызовов API

## Панели мониторинга Grafana

Импортируйте файлы JSON из каталога `grafana/dashboards/` в Grafana.

## Оповещения

Настройте оповещения в Grafana для каждой панели мониторинга в соответствии с требованиями.

## Примечания

- Убедитесь, что PostgreSQL доступен из сети Docker
- Для macOS используйте "host.docker.internal" для подключения к хосту
- Сбор показателей должен выполняться в течение 1-5 часов по мере необходимости
- Все экспортеры должны быть указаны "НА месте" в целях Prometheus