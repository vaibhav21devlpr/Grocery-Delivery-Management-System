'''-- Star Schema Design for Grocery Delivery Data Warehouse
-- Optimized for Analytics and Business Intelligence
-- Author: Vaibhav Pandey
-- Date: April 2026'''


DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS fact_deliveries CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_delivery_personnel CASCADE;


CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season VARCHAR(20)
);


CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    registration_date DATE,
    customer_segment VARCHAR(50),
    is_active BOOLEAN,
    -- SCD Type 2 fields
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);


CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(200),
    category_name VARCHAR(100),
    price DECIMAL(10, 2),
    unit VARCHAR(50),
    is_active BOOLEAN,
    -- SCD Type 2 fields
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);


CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    region VARCHAR(50),
    country VARCHAR(50) DEFAULT 'India'
);


CREATE TABLE dim_delivery_personnel (
    personnel_key SERIAL PRIMARY KEY,
    personnel_id INTEGER NOT NULL,
    full_name VARCHAR(200),
    vehicle_type VARCHAR(50),
    rating DECIMAL(3, 2),
    total_deliveries INTEGER,
    status VARCHAR(20),
    hired_date DATE,
    -- SCD Type 2 fields
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);


CREATE TABLE fact_orders (
    order_fact_key SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    order_date_key INTEGER REFERENCES dim_date(date_key),
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    location_key INTEGER REFERENCES dim_location(location_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    subtotal DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    
    payment_method VARCHAR(50),
    payment_status VARCHAR(20),
    order_status VARCHAR(20),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE fact_deliveries (
    delivery_fact_key SERIAL PRIMARY KEY,
    delivery_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    delivery_date_key INTEGER REFERENCES dim_date(date_key),
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    personnel_key INTEGER REFERENCES dim_delivery_personnel(personnel_key),
    location_key INTEGER REFERENCES dim_location(location_key),
    
    distance_km DECIMAL(6, 2),
    planned_delivery_minutes INTEGER,
    actual_delivery_minutes INTEGER,
    delivery_rating DECIMAL(3, 2),
    
    delivery_delay_minutes INTEGER,
    is_on_time BOOLEAN,
    
    delivery_status VARCHAR(20),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX idx_fact_orders_date ON fact_orders(order_date_key);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_key);
CREATE INDEX idx_fact_orders_product ON fact_orders(product_key);
CREATE INDEX idx_fact_orders_location ON fact_orders(location_key);

CREATE INDEX idx_fact_deliveries_date ON fact_deliveries(delivery_date_key);
CREATE INDEX idx_fact_deliveries_customer ON fact_deliveries(customer_key);
CREATE INDEX idx_fact_deliveries_personnel ON fact_deliveries(personnel_key);
CREATE INDEX idx_fact_deliveries_location ON fact_deliveries(location_key);


CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON dim_customer(is_current);
CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_current ON dim_product(is_current);
CREATE INDEX idx_dim_personnel_id ON dim_delivery_personnel(personnel_id);
CREATE INDEX idx_dim_personnel_current ON dim_delivery_personnel(is_current);


INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month, 
                      day_of_year, week_of_year, month, month_name, quarter, year, 
                      is_weekend, is_holiday, season)
SELECT 
    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER AS date_key,
    date_series AS full_date,
    EXTRACT(DOW FROM date_series) AS day_of_week,
    TO_CHAR(date_series, 'Day') AS day_name,
    EXTRACT(DAY FROM date_series) AS day_of_month,
    EXTRACT(DOY FROM date_series) AS day_of_year,
    EXTRACT(WEEK FROM date_series) AS week_of_year,
    EXTRACT(MONTH FROM date_series) AS month,
    TO_CHAR(date_series, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date_series) AS quarter,
    EXTRACT(YEAR FROM date_series) AS year,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    CASE 
        WHEN EXTRACT(MONTH FROM date_series) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date_series) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date_series) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season
FROM generate_series('2024-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS date_series;


-- Top Selling Products
CREATE MATERIALIZED VIEW mv_top_products AS
SELECT 
    dp.product_name,
    dp.category_name,
    SUM(fo.quantity) AS total_quantity_sold,
    SUM(fo.total_amount) AS total_revenue,
    COUNT(DISTINCT fo.customer_key) AS unique_customers,
    AVG(fo.unit_price) AS avg_price
FROM fact_orders fo
JOIN dim_product dp ON fo.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_name, dp.category_name
ORDER BY total_revenue DESC;

-- Customer Segmentation
CREATE MATERIALIZED VIEW mv_customer_segments AS
SELECT 
    dc.customer_key,
    dc.full_name,
    dc.city,
    dc.state,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.total_amount) AS total_spent,
    AVG(fo.total_amount) AS avg_order_value,
    MAX(dd.full_date) AS last_order_date,
    CASE 
        WHEN SUM(fo.total_amount) > 50000 THEN 'Premium'
        WHEN SUM(fo.total_amount) > 20000 THEN 'Gold'
        WHEN SUM(fo.total_amount) > 10000 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier
FROM dim_customer dc
JOIN fact_orders fo ON dc.customer_key = fo.customer_key
JOIN dim_date dd ON fo.order_date_key = dd.date_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_key, dc.full_name, dc.city, dc.state;

-- Delivery Performance by Personnel
CREATE MATERIALIZED VIEW mv_delivery_performance AS
SELECT 
    dp.full_name AS delivery_person,
    dp.vehicle_type,
    COUNT(fd.delivery_id) AS total_deliveries,
    AVG(fd.actual_delivery_minutes) AS avg_delivery_time,
    AVG(fd.delivery_rating) AS avg_rating,
    SUM(CASE WHEN fd.is_on_time THEN 1 ELSE 0 END)::FLOAT / COUNT(fd.delivery_id) * 100 AS on_time_percentage,
    SUM(fd.distance_km) AS total_distance_km
FROM dim_delivery_personnel dp
JOIN fact_deliveries fd ON dp.personnel_key = fd.personnel_key
WHERE dp.is_current = TRUE
GROUP BY dp.full_name, dp.vehicle_type;

-- Monthly Sales Trends
CREATE MATERIALIZED VIEW mv_monthly_sales AS
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.quantity) AS total_items_sold,
    SUM(fo.total_amount) AS total_revenue,
    AVG(fo.total_amount) AS avg_order_value,
    COUNT(DISTINCT fo.customer_key) AS unique_customers
FROM dim_date dd
JOIN fact_orders fo ON dd.date_key = fo.order_date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- Create indexes on materialized views
CREATE INDEX idx_mv_top_products_revenue ON mv_top_products(total_revenue DESC);
CREATE INDEX idx_mv_customer_segments_tier ON mv_customer_segments(customer_tier);
CREATE INDEX idx_mv_monthly_sales_year_month ON mv_monthly_sales(year, month);

COMMENT ON TABLE fact_orders IS 'Fact table storing order transactions at item level';
COMMENT ON TABLE fact_deliveries IS 'Fact table storing delivery performance metrics';
COMMENT ON TABLE dim_date IS 'Date dimension with calendar attributes';
COMMENT ON TABLE dim_customer IS 'Customer dimension with SCD Type 2 support';
COMMENT ON TABLE dim_product IS 'Product dimension with SCD Type 2 support';
COMMENT ON TABLE dim_location IS 'Location/geography dimension';
COMMENT ON TABLE dim_delivery_personnel IS 'Delivery personnel dimension with SCD Type 2 support';
