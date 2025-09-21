-- Проверки связей (ER) для схемы olist

-- Анти-JOIN: отсутствующие ссылки (должно быть 0 во всех строках)
SELECT 'orders->customers missing' AS check, COUNT(*)
FROM olist.orders o LEFT JOIN olist.customers c ON c.customer_id=o.customer_id
WHERE c.customer_id IS NULL;

SELECT 'order_items->orders missing' AS check, COUNT(*)
FROM olist.order_items oi LEFT JOIN olist.orders o ON o.order_id=oi.order_id
WHERE o.order_id IS NULL;

SELECT 'order_items->products missing' AS check, COUNT(*)
FROM olist.order_items oi LEFT JOIN olist.products p ON p.product_id=oi.product_id
WHERE p.product_id IS NULL;

SELECT 'order_items->sellers missing' AS check, COUNT(*)
FROM olist.order_items oi LEFT JOIN olist.sellers s ON s.seller_id=oi.seller_id
WHERE s.seller_id IS NULL;

SELECT 'order_payments->orders missing' AS check, COUNT(*)
FROM olist.order_payments op LEFT JOIN olist.orders o ON o.order_id=op.order_id
WHERE o.order_id IS NULL;

SELECT 'order_reviews->orders missing' AS check, COUNT(*)
FROM olist.order_reviews r LEFT JOIN olist.orders o ON o.order_id=r.order_id
WHERE o.order_id IS NULL;

-- Подтверждение объёмов JOIN (для отчёта)
SELECT 'orders.customer_id -> customers.customer_id' AS relation, COUNT(*)
FROM olist.orders o JOIN olist.customers c ON c.customer_id=o.customer_id;

SELECT 'order_items.order_id -> orders.order_id' AS relation, COUNT(*)
FROM olist.order_items oi JOIN olist.orders o ON o.order_id=oi.order_id;

SELECT 'order_items.product_id -> products.product_id' AS relation, COUNT(*)
FROM olist.order_items oi JOIN olist.products p ON p.product_id=oi.product_id;

SELECT 'order_items.seller_id -> sellers.seller_id' AS relation, COUNT(*)
FROM olist.order_items oi JOIN olist.sellers s ON s.seller_id=oi.seller_id;

SELECT 'order_payments.order_id -> orders.order_id' AS relation, COUNT(*)
FROM olist.order_payments op JOIN olist.orders o ON o.order_id=op.order_id;

SELECT 'order_reviews.order_id -> orders.order_id' AS relation, COUNT(*)
FROM olist.order_reviews r JOIN olist.orders o ON o.order_id=r.order_id;

-- Дополнительно: мягкие (логические) связи, которые не покрываются FK
-- 1) customers -> geolocation по zip_code_prefix (сколько покрывается координатами)
SELECT 'customers->geolocation missing' AS check, COUNT(*)
FROM olist.customers c LEFT JOIN olist.geolocation g
  ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
WHERE g.geolocation_zip_code_prefix IS NULL;

SELECT 'customers->geolocation coverage_pct' AS metric,
       ROUND(100.0 * COUNT(*) FILTER (WHERE g.geolocation_zip_code_prefix IS NOT NULL) / NULLIF(COUNT(*),0), 2) AS pct
FROM olist.customers c LEFT JOIN olist.geolocation g
  ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix;

-- 2) sellers -> geolocation по zip_code_prefix
SELECT 'sellers->geolocation missing' AS check, COUNT(*)
FROM olist.sellers s LEFT JOIN olist.geolocation g
  ON g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
WHERE g.geolocation_zip_code_prefix IS NULL;

SELECT 'sellers->geolocation coverage_pct' AS metric,
       ROUND(100.0 * COUNT(*) FILTER (WHERE g.geolocation_zip_code_prefix IS NOT NULL) / NULLIF(COUNT(*),0), 2) AS pct
FROM olist.sellers s LEFT JOIN olist.geolocation g
  ON g.geolocation_zip_code_prefix = s.seller_zip_code_prefix;

-- 3) products -> translation по product_category_name (логическая связь)
SELECT 'products->translation missing' AS check, COUNT(*)
FROM olist.products p LEFT JOIN olist.product_category_name_translation t
  ON t.product_category_name = p.product_category_name
WHERE t.product_category_name IS NULL;

SELECT 'products->translation coverage_pct' AS metric,
       ROUND(100.0 * COUNT(*) FILTER (WHERE t.product_category_name IS NOT NULL) / NULLIF(COUNT(*),0), 2) AS pct
FROM olist.products p LEFT JOIN olist.product_category_name_translation t
  ON t.product_category_name = p.product_category_name;
