'''-- Analytical Queries for Grocery Delivery System
-- Business Intelligence and Reporting Queries
-- Author: Vaibhav Pandey'''

-- ============================================
-- SECTION 1: ORDER ANALYTICS
-- ============================================

-- Query 1: Daily Sales Summary
SELECT 
    DATE(order_date) AS order_day,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(final_amount) AS total_revenue,
    AVG(final_amount) AS avg_order_value,
    MAX(final_amount) AS max_order_value
FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(order_date)
ORDER BY order_day DESC;

-- Query 2: Top 10 Best Selling Products
SELECT 
    p.product_name,
    c.category_name,
    COUNT(oi.order_item_id) AS times_ordered,
    SUM(oi.quantity) AS total_quantity,
    SUM(oi.subtotal) AS total_revenue,
    AVG(oi.unit_price) AS avg_price
FROM products p
JOIN categories c ON p.category_id = c.category_id
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.product_id, p.product_name, c.category_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 3: Revenue by Category
SELECT 
    c.category_name,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(oi.quantity) AS total_items_sold,
    SUM(oi.subtotal) AS total_revenue,
    ROUND(SUM(oi.subtotal) * 100.0 / SUM(SUM(oi.subtotal)) OVER(), 2) AS revenue_percentage
FROM categories c
JOIN products p ON c.category_id = p.category_id
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY c.category_id, c.category_name
ORDER BY total_revenue DESC;

-- ============================================
-- SECTION 2: CUSTOMER ANALYTICS
-- ============================================

-- Query 4: Customer Lifetime Value (CLV)
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.final_amount) AS lifetime_value,
    AVG(o.final_amount) AS avg_order_value,
    MAX(o.order_date) AS last_order_date,
    EXTRACT(DAY FROM CURRENT_DATE - MAX(o.order_date)) AS days_since_last_order
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, customer_name, c.email
ORDER BY lifetime_value DESC
LIMIT 20;

-- Query 5: Customer Segmentation (RFM Analysis)
WITH rfm_data AS (
    SELECT 
        customer_id,
        EXTRACT(DAY FROM CURRENT_DATE - MAX(order_date)) AS recency,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(final_amount) AS monetary
    FROM orders
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT 
        customer_id,
        recency,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM rfm_data
)
SELECT 
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    r.recency AS days_since_last_purchase,
    r.frequency AS total_orders,
    r.monetary AS total_spent,
    r.r_score,
    r.f_score,
    r.m_score,
    CASE 
        WHEN r.r_score >= 4 AND r.f_score >= 4 AND r.m_score >= 4 THEN 'Champions'
        WHEN r.r_score >= 3 AND r.f_score >= 3 THEN 'Loyal Customers'
        WHEN r.r_score >= 4 AND r.f_score <= 2 THEN 'Promising'
        WHEN r.r_score <= 2 AND r.f_score >= 4 THEN 'At Risk'
        WHEN r.r_score <= 2 AND r.f_score <= 2 THEN 'Lost'
        ELSE 'Regular'
    END AS customer_segment
FROM rfm_scores r
JOIN customers c ON r.customer_id = c.customer_id
ORDER BY r.m_score DESC, r.f_score DESC;

-- Query 6: Repeat Purchase Rate
SELECT 
    COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END)::FLOAT / 
    COUNT(DISTINCT customer_id) * 100 AS repeat_purchase_rate_percentage,
    COUNT(DISTINCT CASE WHEN order_count = 1 THEN customer_id END) AS one_time_customers,
    COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) AS repeat_customers
FROM (
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
) AS customer_orders;

-- ============================================
-- SECTION 3: DELIVERY ANALYTICS
-- ============================================

-- Query 7: Delivery Performance Metrics
SELECT 
    dp.first_name || ' ' || dp.last_name AS delivery_person,
    dp.vehicle_type,
    COUNT(d.delivery_id) AS total_deliveries,
    AVG(EXTRACT(EPOCH FROM (d.actual_delivery_time - d.pickup_time))/60) AS avg_delivery_minutes,
    AVG(d.delivery_rating) AS avg_rating,
    SUM(CASE WHEN d.actual_delivery_time <= d.estimated_delivery_time THEN 1 ELSE 0 END)::FLOAT / 
        COUNT(d.delivery_id) * 100 AS on_time_percentage,
    AVG(d.distance_km) AS avg_distance_km
FROM delivery_personnel dp
JOIN deliveries d ON dp.personnel_id = d.personnel_id
WHERE d.delivery_status = 'delivered'
GROUP BY dp.personnel_id, delivery_person, dp.vehicle_type
HAVING COUNT(d.delivery_id) >= 5
ORDER BY on_time_percentage DESC, avg_rating DESC;

-- Query 8: Delivery Time Analysis by Hour
SELECT 
    EXTRACT(HOUR FROM d.pickup_time) AS delivery_hour,
    COUNT(d.delivery_id) AS total_deliveries,
    AVG(EXTRACT(EPOCH FROM (d.actual_delivery_time - d.pickup_time))/60) AS avg_delivery_minutes,
    AVG(d.distance_km) AS avg_distance_km
FROM deliveries d
WHERE d.delivery_status = 'delivered'
    AND d.pickup_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY delivery_hour
ORDER BY delivery_hour;

-- Query 9: Delivery Success Rate by City
SELECT 
    o.delivery_city,
    COUNT(d.delivery_id) AS total_deliveries,
    SUM(CASE WHEN d.delivery_status = 'delivered' THEN 1 ELSE 0 END) AS successful_deliveries,
    SUM(CASE WHEN d.delivery_status = 'failed' THEN 1 ELSE 0 END) AS failed_deliveries,
    ROUND(SUM(CASE WHEN d.delivery_status = 'delivered' THEN 1 ELSE 0 END)::NUMERIC / 
          COUNT(d.delivery_id) * 100, 2) AS success_rate_percentage
FROM orders o
JOIN deliveries d ON o.order_id = d.order_id
WHERE o.delivery_city IS NOT NULL
GROUP BY o.delivery_city
HAVING COUNT(d.delivery_id) >= 10
ORDER BY success_rate_percentage DESC;

-- ============================================
-- SECTION 4: INVENTORY ANALYTICS
-- ============================================

-- Query 10: Low Stock Alert
SELECT 
    p.product_name,
    c.category_name,
    i.quantity_available,
    i.quantity_reserved,
    i.reorder_level,
    i.quantity_available - i.reorder_level AS stock_difference,
    CASE 
        WHEN i.quantity_available <= 0 THEN 'OUT_OF_STOCK'
        WHEN i.quantity_available <= i.reorder_level THEN 'LOW_STOCK'
        ELSE 'SUFFICIENT'
    END AS stock_status
FROM inventory i
JOIN products p ON i.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE i.quantity_available <= i.reorder_level
ORDER BY stock_difference ASC;

-- Query 11: Inventory Turnover Rate
WITH sales_data AS (
    SELECT 
        oi.product_id,
        SUM(oi.quantity) AS total_sold
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY oi.product_id
)
SELECT 
    p.product_name,
    c.category_name,
    i.quantity_available AS current_stock,
    COALESCE(s.total_sold, 0) AS sold_last_30_days,
    CASE 
        WHEN i.quantity_available > 0 
        THEN ROUND(COALESCE(s.total_sold, 0)::NUMERIC / i.quantity_available, 2)
        ELSE 0
    END AS turnover_rate
FROM products p
JOIN categories c ON p.category_id = c.category_id
JOIN inventory i ON p.product_id = i.product_id
LEFT JOIN sales_data s ON p.product_id = s.product_id
WHERE p.is_active = TRUE
ORDER BY turnover_rate DESC;

-- ============================================
-- SECTION 5: TIME-BASED ANALYTICS
-- ============================================

-- Query 12: Peak Order Times
SELECT 
    EXTRACT(HOUR FROM order_date) AS order_hour,
    EXTRACT(DOW FROM order_date) AS day_of_week,
    CASE EXTRACT(DOW FROM order_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    COUNT(*) AS order_count,
    SUM(final_amount) AS total_revenue
FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY order_hour, day_of_week, day_name
ORDER BY order_count DESC
LIMIT 20;

-- Query 13: Month-over-Month Growth
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(final_amount) AS total_revenue
    FROM orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY month
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') AS month,
    total_orders,
    total_revenue,
    LAG(total_revenue) OVER (ORDER BY month) AS prev_month_revenue,
    ROUND(((total_revenue - LAG(total_revenue) OVER (ORDER BY month)) / 
           LAG(total_revenue) OVER (ORDER BY month) * 100), 2) AS revenue_growth_percentage
FROM monthly_sales
ORDER BY month DESC;

-- ============================================
-- SECTION 6: ADVANCED ANALYTICS
-- ============================================

-- Query 14: Product Affinity Analysis (Market Basket)
SELECT 
    p1.product_name AS product_1,
    p2.product_name AS product_2,
    COUNT(DISTINCT oi1.order_id) AS times_bought_together,
    ROUND(COUNT(DISTINCT oi1.order_id)::NUMERIC / 
          (SELECT COUNT(DISTINCT order_id) FROM order_items) * 100, 2) AS support_percentage
FROM order_items oi1
JOIN order_items oi2 ON oi1.order_id = oi2.order_id AND oi1.product_id < oi2.product_id
JOIN products p1 ON oi1.product_id = p1.product_id
JOIN products p2 ON oi2.product_id = p2.product_id
GROUP BY p1.product_id, p1.product_name, p2.product_id, p2.product_name
HAVING COUNT(DISTINCT oi1.order_id) >= 5
ORDER BY times_bought_together DESC
LIMIT 20;

-- Query 15: Customer Churn Prediction
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    MAX(o.order_date) AS last_order_date,
    EXTRACT(DAY FROM CURRENT_DATE - MAX(o.order_date)) AS days_since_last_order,
    COUNT(o.order_id) AS total_orders,
    SUM(o.final_amount) AS total_spent,
    CASE 
        WHEN EXTRACT(DAY FROM CURRENT_DATE - MAX(o.order_date)) > 90 THEN 'High Risk'
        WHEN EXTRACT(DAY FROM CURRENT_DATE - MAX(o.order_date)) > 60 THEN 'Medium Risk'
        WHEN EXTRACT(DAY FROM CURRENT_DATE - MAX(o.order_date)) > 30 THEN 'Low Risk'
        ELSE 'Active'
    END AS churn_risk
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, customer_name, c.email
HAVING COUNT(o.order_id) > 1
ORDER BY days_since_last_order DESC;

-- Query 16: Geographic Revenue Analysis
SELECT 
    delivery_city,
    delivery_zip,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(final_amount) AS total_revenue,
    AVG(final_amount) AS avg_order_value,
    ROUND(SUM(final_amount)::NUMERIC / SUM(SUM(final_amount)) OVER() * 100, 2) AS revenue_contribution_percentage
FROM orders
WHERE delivery_city IS NOT NULL
GROUP BY delivery_city, delivery_zip
HAVING COUNT(DISTINCT order_id) >= 10
ORDER BY total_revenue DESC;

-- Query 17: Payment Method Analysis
SELECT 
    payment_method,
    COUNT(*) AS transaction_count,
    SUM(final_amount) AS total_amount,
    AVG(final_amount) AS avg_transaction_value,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER() * 100, 2) AS usage_percentage
FROM orders
WHERE payment_status = 'completed'
GROUP BY payment_method
ORDER BY transaction_count DESC;
