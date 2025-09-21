-- Обзор схемы и типов столбцов по ключевым таблицам olist

-- Таблицы
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema='olist'
ORDER BY table_name;

-- Колонки с типами (orders)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='olist' AND table_name='orders'
ORDER BY ordinal_position;

-- Колонки с типами (order_items)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='olist' AND table_name='order_items'
ORDER BY ordinal_position;

-- Колонки с типами (order_payments)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='olist' AND table_name='order_payments'
ORDER BY ordinal_position;

-- Колонки с типами (order_reviews)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='olist' AND table_name='order_reviews'
ORDER BY ordinal_position;

-- Колонки с типами (products)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='olist' AND table_name='products'
ORDER BY ordinal_position;

-- Колонки с типами (customers)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='olist' AND table_name='customers'
ORDER BY ordinal_position;

-- Колонки с типами (sellers)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='olist' AND table_name='sellers'
ORDER BY ordinal_position;

