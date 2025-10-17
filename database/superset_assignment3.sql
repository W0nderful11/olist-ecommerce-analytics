-- Superset Assignment #3 Helper SQL Objects
-- Execute (psql -f) after base import to create views for easier visualization building.

-- 1. Geo view: enrich seller + customer states with average lat/lng (centroid style)
DROP VIEW IF EXISTS olist.v_geo_customer_state CASCADE;
CREATE VIEW olist.v_geo_customer_state AS
SELECT c.customer_state AS state,
       AVG(g.geolocation_lat)  AS lat,
       AVG(g.geolocation_lng)  AS lng,
       COUNT(DISTINCT c.customer_id) AS customers
FROM olist.customers c
LEFT JOIN olist.geolocation g
  ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
GROUP BY c.customer_state;

-- 2. Heatmap (category x state) by GMV
DROP VIEW IF EXISTS olist.v_heatmap_category_state CASCADE;
CREATE VIEW olist.v_heatmap_category_state AS
SELECT COALESCE(t.product_category_name_english, p.product_category_name) AS category,
       c.customer_state AS state,
       SUM(oi.price + oi.freight_value) AS gmv,
       COUNT(*) AS items
FROM olist.order_items oi
JOIN olist.orders o      ON o.order_id = oi.order_id
JOIN olist.customers c   ON c.customer_id = o.customer_id
JOIN olist.products p    ON p.product_id = oi.product_id
LEFT JOIN olist.product_category_name_translation t
       ON t.product_category_name = p.product_category_name
GROUP BY category, c.customer_state;

-- 3. Sunburst (state -> category -> seller) using revenue
DROP VIEW IF EXISTS olist.v_sunburst_state_category_seller CASCADE;
CREATE VIEW olist.v_sunburst_state_category_seller AS
SELECT c.customer_state AS customer_state,
       COALESCE(t.product_category_name_english, p.product_category_name) AS category,
       s.seller_id,
       SUM(oi.price + oi.freight_value) AS revenue
FROM olist.order_items oi
JOIN olist.orders o    ON o.order_id = oi.order_id
JOIN olist.customers c ON c.customer_id = o.customer_id
JOIN olist.products p  ON p.product_id = oi.product_id
JOIN olist.sellers s   ON s.seller_id = oi.seller_id
LEFT JOIN olist.product_category_name_translation t
       ON t.product_category_name = p.product_category_name
GROUP BY c.customer_state, category, s.seller_id;

-- 4. Treemap (category -> state) by GMV (limited to top N categories for clarity)
DROP VIEW IF EXISTS olist.v_treemap_category_state CASCADE;
CREATE VIEW olist.v_treemap_category_state AS
WITH cat_totals AS (
  SELECT COALESCE(t.product_category_name_english, p.product_category_name) AS category,
         SUM(oi.price + oi.freight_value) AS gmv
  FROM olist.order_items oi
  JOIN olist.products p ON p.product_id = oi.product_id
  LEFT JOIN olist.product_category_name_translation t ON t.product_category_name = p.product_category_name
  GROUP BY 1
), top_cat AS (
  SELECT category FROM cat_totals ORDER BY gmv DESC LIMIT 20
)
SELECT tc.category,
       c.customer_state AS state,
       SUM(oi.price + oi.freight_value) AS gmv
FROM olist.order_items oi
JOIN olist.orders o    ON o.order_id = oi.order_id
JOIN olist.customers c ON c.customer_id = o.customer_id
JOIN olist.products p  ON p.product_id = oi.product_id
LEFT JOIN olist.product_category_name_translation t ON t.product_category_name = p.product_category_name
JOIN top_cat tc ON tc.category = COALESCE(t.product_category_name_english, p.product_category_name)
GROUP BY tc.category, c.customer_state;

-- 5. Word cloud source: product category frequency
DROP VIEW IF EXISTS olist.v_wordcloud_category_frequency CASCADE;
CREATE VIEW olist.v_wordcloud_category_frequency AS
SELECT COALESCE(t.product_category_name_english, p.product_category_name) AS category,
       COUNT(*) AS frequency
FROM olist.order_items oi
JOIN olist.products p ON p.product_id = oi.product_id
LEFT JOIN olist.product_category_name_translation t ON t.product_category_name = p.product_category_name
GROUP BY 1;

-- 6. Order totals enriched (for normalization + categorization tasks)
DROP VIEW IF EXISTS olist.v_order_totals_enriched CASCADE;
CREATE VIEW olist.v_order_totals_enriched AS
WITH totals AS (
  SELECT o.order_id,
         o.customer_id,
         SUM(oi.price + oi.freight_value) AS order_total,
         date_trunc('month', o.order_purchase_timestamp)::date AS month
  FROM olist.orders o
  JOIN olist.order_items oi ON oi.order_id = o.order_id
  GROUP BY o.order_id, o.customer_id
), stats AS (
  SELECT MIN(order_total) AS min_total,
         MAX(order_total) AS max_total
  FROM totals
)
SELECT t.order_id,
       t.customer_id,
       t.order_total,
       t.month,
       CASE WHEN s.max_total > s.min_total THEN (t.order_total - s.min_total) / NULLIF(s.max_total - s.min_total,0)
            ELSE 0 END AS order_total_normalized,
       CASE
         WHEN t.order_total < 50 THEN 'LOW'
         WHEN t.order_total < 150 THEN 'MEDIUM'
         WHEN t.order_total < 300 THEN 'HIGH'
         ELSE 'ULTRA'
       END AS order_total_segment
FROM totals t CROSS JOIN stats s;

-- 7. Monthly GMV live (for comparison with CSV snapshot in task 10)
DROP VIEW IF EXISTS olist.v_monthly_gmv_live CASCADE;
CREATE VIEW olist.v_monthly_gmv_live AS
SELECT date_trunc('month', o.order_purchase_timestamp)::date AS month,
       SUM(oi.price + oi.freight_value) AS gmv
FROM olist.orders o
JOIN olist.order_items oi ON oi.order_id = o.order_id
GROUP BY 1;

-- 8. Metrics base view (month over month growth computation in Superset metrics UI)
DROP VIEW IF EXISTS olist.v_monthly_gmv_metrics CASCADE;
CREATE VIEW olist.v_monthly_gmv_metrics AS
SELECT m.month,
       m.gmv,
       LAG(m.gmv) OVER (ORDER BY m.month) AS prev_gmv
FROM olist.v_monthly_gmv_live m;

-- 9. Live GMV per minute (raw, not densified)
--    Use this when you prefer Superset's resample/fill to handle gaps.
DROP VIEW IF EXISTS olist.v_live_gmv_minute CASCADE;
CREATE VIEW olist.v_live_gmv_minute AS
SELECT date_trunc('minute', o.order_purchase_timestamp)::timestamp AS ts_minute,
       SUM(oi.price + oi.freight_value) AS gmv_minute
FROM olist.orders o
JOIN olist.order_items oi ON oi.order_id = o.order_id
GROUP BY 1;

-- 10. Live GMV per minute — densified window for the last 3 hours, with cumulative sum
--     This view ensures every minute exists (gmv_minute defaults to 0) so the line "keeps going".
DROP VIEW IF EXISTS olist.v_live_gmv_minute_3h CASCADE;
CREATE VIEW olist.v_live_gmv_minute_3h AS
WITH mins AS (
  SELECT generate_series(
           date_trunc('minute', now() - interval '3 hours'),
           date_trunc('minute', now()),
           interval '1 minute'
         )::timestamp AS ts_minute
), agg AS (
  SELECT date_trunc('minute', o.order_purchase_timestamp)::timestamp AS ts_minute,
         SUM(oi.price + oi.freight_value) AS gmv_minute
  FROM olist.orders o
  JOIN olist.order_items oi ON oi.order_id = o.order_id
  GROUP BY 1
)
SELECT m.ts_minute,
       COALESCE(a.gmv_minute, 0) AS gmv_minute,
       SUM(COALESCE(a.gmv_minute, 0)) OVER (ORDER BY m.ts_minute) AS gmv_cum
FROM mins m
LEFT JOIN agg a USING (ts_minute)
ORDER BY m.ts_minute;

-- Index helpers (optional for faster dashboarding)
CREATE INDEX IF NOT EXISTS idx_v_order_totals_enriched_month ON olist.orders(order_purchase_timestamp);
-- (Other indexes exist on base tables already.)

-- End of superset_assignment3.sql
