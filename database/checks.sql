-- Базовые проверки по методичке

-- 1) Просмотр первых 10 строк из каждой ключевой таблицы
SELECT * FROM olist.customers LIMIT 10;
SELECT * FROM olist.orders LIMIT 10;
SELECT * FROM olist.order_items LIMIT 10;
SELECT * FROM olist.order_payments LIMIT 10;
SELECT * FROM olist.order_reviews LIMIT 10;
SELECT * FROM olist.products LIMIT 10;
SELECT * FROM olist.sellers LIMIT 10;

-- 2) WHERE + ORDER BY: последние одобренные заказы
SELECT order_id, customer_id, order_status, order_purchase_timestamp
FROM olist.orders
WHERE order_status IN ('delivered','shipped','invoiced')
ORDER BY order_purchase_timestamp DESC
LIMIT 20;

-- 3) GROUP BY + COUNT/AVG/MIN/MAX: базовые агрегаты по типам оплаты
SELECT payment_type,
       COUNT(*) AS cnt,
       AVG(payment_value) AS avg_payment,
       MIN(payment_value) AS min_payment,
       MAX(payment_value) AS max_payment
FROM olist.order_payments
GROUP BY payment_type
ORDER BY cnt DESC;

-- 4) JOIN: сумма по заказу через items
SELECT o.order_id,
       ROUND(SUM(oi.price + oi.freight_value)::NUMERIC,2) AS order_total
FROM olist.orders o
JOIN olist.order_items oi ON oi.order_id = o.order_id
GROUP BY o.order_id
ORDER BY order_total DESC
LIMIT 20;


