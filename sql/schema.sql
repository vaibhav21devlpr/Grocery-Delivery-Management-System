'''-- Database Schema for Grocery Delivery Management System
-- Author: Vaibhav Pandey
-- Date: April 2026'''

DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS deliveries CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS delivery_personnel CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;

CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category_id INTEGER REFERENCES categories(category_id),
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    unit VARCHAR(50),
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(200) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_order_date TIMESTAMP,
    total_orders INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE delivery_personnel (
    personnel_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(50),
    license_number VARCHAR(50),
    rating DECIMAL(3, 2) CHECK (rating >= 0 AND rating <= 5),
    total_deliveries INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'available',
    hired_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount >= 0),
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    tax_amount DECIMAL(10, 2) DEFAULT 0,
    final_amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50),
    payment_status VARCHAR(20) DEFAULT 'pending',
    order_status VARCHAR(20) DEFAULT 'pending',
    delivery_address TEXT NOT NULL,
    delivery_city VARCHAR(100),
    delivery_zip VARCHAR(20),
    special_instructions TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE deliveries (
    delivery_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    personnel_id INTEGER REFERENCES delivery_personnel(personnel_id),
    pickup_time TIMESTAMP,
    estimated_delivery_time TIMESTAMP,
    actual_delivery_time TIMESTAMP,
    delivery_status VARCHAR(20) DEFAULT 'pending',
    distance_km DECIMAL(6, 2),
    delivery_rating DECIMAL(3, 2) CHECK (delivery_rating >= 0 AND delivery_rating <= 5),
    customer_feedback TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    quantity_available INTEGER NOT NULL CHECK (quantity_available >= 0),
    quantity_reserved INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 50,
    last_restock_date TIMESTAMP,
    warehouse_location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_deliveries_order ON deliveries(order_id);
CREATE INDEX idx_deliveries_personnel ON deliveries(personnel_id);
CREATE INDEX idx_deliveries_status ON deliveries(delivery_status);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_inventory_product ON inventory(product_id);



CREATE OR REPLACE VIEW active_orders_summary AS
SELECT 
    o.order_id,
    o.order_date,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    o.final_amount,
    o.order_status,
    d.delivery_status,
    dp.first_name || ' ' || dp.last_name AS delivery_person
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN deliveries d ON o.order_id = d.order_id
LEFT JOIN delivery_personnel dp ON d.personnel_id = dp.personnel_id
WHERE o.order_status NOT IN ('delivered', 'cancelled');

CREATE OR REPLACE VIEW product_sales_summary AS
SELECT 
    p.product_id,
    p.product_name,
    cat.category_name,
    COUNT(oi.order_item_id) AS times_ordered,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.subtotal) AS total_revenue
FROM products p
JOIN categories cat ON p.category_id = cat.category_id
LEFT JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, cat.category_name;

CREATE OR REPLACE VIEW delivery_performance AS
SELECT 
    dp.personnel_id,
    dp.first_name || ' ' || dp.last_name AS delivery_person,
    COUNT(d.delivery_id) AS total_deliveries,
    AVG(EXTRACT(EPOCH FROM (d.actual_delivery_time - d.pickup_time))/60) AS avg_delivery_time_minutes,
    AVG(d.delivery_rating) AS avg_rating,
    SUM(CASE WHEN d.actual_delivery_time <= d.estimated_delivery_time THEN 1 ELSE 0 END)::FLOAT / 
        COUNT(d.delivery_id) * 100 AS on_time_percentage
FROM delivery_personnel dp
LEFT JOIN deliveries d ON dp.personnel_id = d.personnel_id
WHERE d.delivery_status = 'delivered'
GROUP BY dp.personnel_id, dp.first_name, dp.last_name;



CREATE OR REPLACE FUNCTION update_customer_stats()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE customers
    SET total_orders = (SELECT COUNT(*) FROM orders WHERE customer_id = NEW.customer_id),
        last_order_date = NEW.order_date
    WHERE customer_id = NEW.customer_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_customer_stats ON orders;
CREATE TRIGGER trg_update_customer_stats
AFTER INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION update_customer_stats();


CREATE OR REPLACE FUNCTION check_inventory()
RETURNS TRIGGER AS $$
DECLARE
    available_qty INTEGER;
BEGIN
    SELECT quantity_available INTO available_qty
    FROM inventory
    WHERE product_id = NEW.product_id;
    
    IF available_qty < NEW.quantity THEN
        RAISE EXCEPTION 'Insufficient inventory for product_id %', NEW.product_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS trg_check_inventory ON order_items;
CREATE TRIGGER trg_check_inventory
BEFORE INSERT ON order_items
FOR EACH ROW
EXECUTE FUNCTION check_inventory();

COMMENT ON TABLE categories IS 'Product categories (Fruits, Vegetables, Dairy, etc.)';
COMMENT ON TABLE products IS 'Individual products available for purchase';
COMMENT ON TABLE customers IS 'Customer information and profile';
COMMENT ON TABLE orders IS 'Customer orders with payment and status';
COMMENT ON TABLE order_items IS 'Individual items within each order';
COMMENT ON TABLE deliveries IS 'Delivery tracking and performance';
COMMENT ON TABLE delivery_personnel IS 'Delivery staff information';
COMMENT ON TABLE inventory IS 'Product stock levels and warehouse management';
