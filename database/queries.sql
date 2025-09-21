-- Olist — 10 аналитических запросов
-- Файл содержит тематические запросы для проверки и аналитики.
-- Каждая секция начинается с комментария, объясняющего смысл.

-- 1) GMV по штатам клиентов
SELECT c.customer_state AS state,
       ROUND(SUM(oi.price + oi.freight_value)::NUMERIC,2) AS gmv
FROM olist.orders o
JOIN olist.customers c ON c.customer_id = o.customer_id
JOIN olist.order_items oi ON oi.order_id = o.order_id
GROUP BY state
ORDER BY gmv DESC;

-- 2) Топ категорий по выручке
SELECT COALESCE(t.product_category_name_english, p.product_category_name) AS category,
       ROUND(SUM(oi.price)::NUMERIC,2) AS revenue,
       COUNT(*) AS items_sold
FROM olist.order_items oi
JOIN olist.products p ON p.product_id = oi.product_id
LEFT JOIN olist.product_category_name_translation t ON t.product_category_name = p.product_category_name
GROUP BY category
ORDER BY revenue DESC
LIMIT 15;

-- 3) Месячная динамика GMV (12м)
SELECT date_trunc('month', o.order_purchase_timestamp)::date AS month,
       ROUND(SUM(oi.price + oi.freight_value)::NUMERIC,2) AS gmv
FROM olist.orders o
JOIN olist.order_items oi ON oi.order_id = o.order_id
WHERE o.order_purchase_timestamp >= (current_date - INTERVAL '12 months')
GROUP BY 1
ORDER BY 1;

-- 4) Средний чек по клиентам (топ‑20)
SELECT x.customer_id, c.customer_city, c.customer_state, COUNT(*) AS orders_count,
       ROUND(AVG(x.order_total)::NUMERIC,2) AS avg_order_value
FROM (
  SELECT o.order_id, o.customer_id, SUM(oi.price + oi.freight_value) AS order_total
  FROM olist.orders o
  JOIN olist.order_items oi ON oi.order_id = o.order_id
  GROUP BY o.order_id, o.customer_id
) x
JOIN olist.customers c ON c.customer_id = x.customer_id
GROUP BY x.customer_id, c.customer_city, c.customer_state
ORDER BY avg_order_value DESC
LIMIT 20;

-- 5) Доля и объём по типам оплаты
SELECT payment_type,
       COUNT(*) AS payment_records,
       ROUND(SUM(payment_value)::NUMERIC,2) AS total_paid,
       ROUND(100.0 * SUM(payment_value) / NULLIF((SELECT SUM(payment_value) FROM olist.order_payments),0),2) AS pct_of_total
FROM olist.order_payments
GROUP BY payment_type
ORDER BY total_paid DESC;

-- 6) Среднее время доставки (дни) по штатам
SELECT c.customer_state,
       ROUND(AVG(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0)::NUMERIC,2) AS avg_delivery_days,
       COUNT(*) AS delivered_orders
FROM olist.orders o
JOIN olist.customers c ON c.customer_id = o.customer_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delivery_days;

-- 7) Доля повторных покупателей
SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE orders_count >= 2) / NULLIF(COUNT(*),0),2) AS repeat_purchase_rate_pct,
       COUNT(*) FILTER (WHERE orders_count >= 2) AS repeat_customers,
       COUNT(*) AS total_customers
FROM (
  SELECT customer_id, COUNT(*) AS orders_count
  FROM olist.orders
  GROUP BY customer_id
) t;

-- 8) Топ продавцов по выручке
SELECT oi.seller_id,
       COUNT(DISTINCT oi.order_id) AS orders,
       ROUND(SUM(oi.price)::NUMERIC,2) AS revenue
FROM olist.order_items oi
GROUP BY oi.seller_id
ORDER BY revenue DESC
LIMIT 20;

-- 9) Средний рейтинг отзывов по категориям (>=50 отзывов)
SELECT COALESCE(t.product_category_name_english, p.product_category_name) AS category,
       ROUND(AVG(r.review_score)::NUMERIC,2) AS avg_review,
       COUNT(*) AS reviews
FROM olist.order_reviews r
JOIN olist.order_items oi ON oi.order_id = r.order_id
JOIN olist.products p ON p.product_id = oi.product_id
LEFT JOIN olist.product_category_name_translation t ON t.product_category_name = p.product_category_name
GROUP BY category
HAVING COUNT(*) >= 50
ORDER BY avg_review DESC
LIMIT 20;

-- 10) Доля отменённых заказов по штатам
SELECT c.customer_state,
       COUNT(*) FILTER (WHERE o.order_status = 'canceled') AS canceled,
       COUNT(*) AS total_orders,
       ROUND(100.0 * COUNT(*) FILTER (WHERE o.order_status = 'canceled') / NULLIF(COUNT(*),0),2) AS canceled_rate_pct
FROM olist.orders o
JOIN olist.customers c ON c.customer_id = o.customer_id
GROUP BY c.customer_state
ORDER BY canceled_rate_pct DESC NULLS LAST;

-- 11) Минимальная корзина по категориям (пример MIN/MAX)
SELECT COALESCE(t.product_category_name_english, p.product_category_name) AS category,
       MIN(oi.price) AS min_item_price,
       MAX(oi.price) AS max_item_price
FROM olist.order_items oi
JOIN olist.products p ON p.product_id = oi.product_id
LEFT JOIN olist.product_category_name_translation t ON t.product_category_name = p.product_category_name
GROUP BY category
ORDER BY category;

