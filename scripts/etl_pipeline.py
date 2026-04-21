"""
ETL Pipeline for Grocery Delivery Management System
Extracts data from CSV, Transforms it, and Loads into PostgreSQL
Author: Vaibhav Pandey
Date: April 2026
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import yaml
import logging
from datetime import datetime
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ETLPipeline:
    """ETL Pipeline for loading data into PostgreSQL"""
    
    def __init__(self, config_file='config/database.yaml'):
        self.config = self.load_config(config_file)
        self.conn = None
        self.cursor = None
        
    def load_config(self, config_file):
        """Load database configuration"""
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info("Configuration loaded successfully")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            sys.exit(1)
    
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            db_config = self.config['database']
            self.conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            sys.exit(1)
    
    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def extract_data(self, file_path):
        """Extract data from CSV file"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Extracted {len(df)} records from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error extracting data from {file_path}: {e}")
            return None
    
    def transform_data(self, df, dataset_name):
        """Transform data based on business rules"""
        logger.info(f"Transforming {dataset_name} data...")
        
        df = df.fillna({col: '' if df[col].dtype == 'object' else 0 for col in df.columns})
        
        bool_columns = df.select_dtypes(include=['object']).columns
        for col in bool_columns:
            if df[col].isin(['True', 'False', 'TRUE', 'FALSE', 'true', 'false']).any():
                df[col] = df[col].map({'True': True, 'False': False, 
                                       'TRUE': True, 'FALSE': False,
                                       'true': True, 'false': False})
        
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
        
        logger.info(f"Transformation complete for {dataset_name}")
        return df
    
    def load_categories(self, df):
        """Load categories into database"""
        logger.info("Loading categories...")
        
        query = """
        INSERT INTO categories (category_id, category_name, description)
        VALUES (%s, %s, %s)
        ON CONFLICT (category_name) DO NOTHING;
        """
        
        data = [tuple(row) for row in df[['category_id', 'category_name', 'description']].values]
        
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            logger.info(f"Loaded {len(data)} categories")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading categories: {e}")
    
    def load_products(self, df):
        """Load products into database"""
        logger.info("Loading products...")
        
        query = """
        INSERT INTO products (product_id, product_name, category_id, price, unit, description, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            price = EXCLUDED.price,
            is_active = EXCLUDED.is_active;
        """
        
        data = [tuple(row) for row in df[['product_id', 'product_name', 'category_id', 
                                          'price', 'unit', 'description', 'is_active']].values]
        
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            logger.info(f"Loaded {len(data)} products")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading products: {e}")
    
    def load_customers(self, df):
        """Load customers into database"""
        logger.info("Loading customers...")
        
        query = """
        INSERT INTO customers (customer_id, first_name, last_name, email, phone, address, 
                              city, state, zip_code, registration_date, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (email) DO UPDATE SET
            phone = EXCLUDED.phone,
            address = EXCLUDED.address,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            zip_code = EXCLUDED.zip_code;
        """
        
        data = [tuple(row) for row in df[['customer_id', 'first_name', 'last_name', 'email', 
                                          'phone', 'address', 'city', 'state', 'zip_code', 
                                          'registration_date', 'is_active']].values]
        
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            logger.info(f"Loaded {len(data)} customers")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading customers: {e}")
    
    def load_delivery_personnel(self, df):
        """Load delivery personnel into database"""
        logger.info("Loading delivery personnel...")
        
        query = """
        INSERT INTO delivery_personnel (personnel_id, first_name, last_name, phone, vehicle_type, 
                                       license_number, rating, total_deliveries, status, hired_date, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (personnel_id) DO UPDATE SET
            rating = EXCLUDED.rating,
            total_deliveries = EXCLUDED.total_deliveries,
            status = EXCLUDED.status;
        """
        
        data = [tuple(row) for row in df[['personnel_id', 'first_name', 'last_name', 'phone', 
                                          'vehicle_type', 'license_number', 'rating', 
                                          'total_deliveries', 'status', 'hired_date', 'is_active']].values]
        
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            logger.info(f"Loaded {len(data)} delivery personnel")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading delivery personnel: {e}")
    
    def load_orders(self, df):
        """Load orders into database"""
        logger.info("Loading orders...")
        
        query = """
        INSERT INTO orders (order_id, customer_id, order_date, total_amount, discount_amount, 
                           tax_amount, final_amount, payment_method, payment_status, order_status,
                           delivery_address, delivery_city, delivery_zip, special_instructions)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE SET
            order_status = EXCLUDED.order_status,
            payment_status = EXCLUDED.payment_status;
        """
        
        data = [tuple(row) for row in df[['order_id', 'customer_id', 'order_date', 'total_amount', 
                                          'discount_amount', 'tax_amount', 'final_amount', 
                                          'payment_method', 'payment_status', 'order_status',
                                          'delivery_address', 'delivery_city', 'delivery_zip', 
                                          'special_instructions']].values]
        
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            logger.info(f"Loaded {len(data)} orders")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading orders: {e}")
    
    def load_order_items(self, df):
        """Load order items into database"""
        logger.info("Loading order items...")
        
        query = """
        INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, subtotal)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_item_id) DO NOTHING;
        """
        
        data = [tuple(row) for row in df[['order_item_id', 'order_id', 'product_id', 
                                          'quantity', 'unit_price', 'subtotal']].values]
        
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            logger.info(f"Loaded {len(data)} order items")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading order items: {e}")
    
    def load_deliveries(self, df):
        """Load deliveries into database"""
        logger.info("Loading deliveries...")
        
        # Convert datetime columns properly
        df['pickup_time'] = pd.to_datetime(df['pickup_time'], errors='coerce')
        df['estimated_delivery_time'] = pd.to_datetime(df['estimated_delivery_time'], errors='coerce')
        df['actual_delivery_time'] = pd.to_datetime(df['actual_delivery_time'], errors='coerce')

        df['delivery_rating'] = pd.to_numeric(df['delivery_rating'], errors='coerce')

        df = df.replace({pd.NaT: None})

        query = """
        INSERT INTO deliveries (delivery_id, order_id, personnel_id, pickup_time, 
                            estimated_delivery_time, actual_delivery_time, delivery_status,
                            distance_km, delivery_rating, customer_feedback)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (delivery_id) DO UPDATE SET
            delivery_status = EXCLUDED.delivery_status,
            actual_delivery_time = EXCLUDED.actual_delivery_time,
            delivery_rating = EXCLUDED.delivery_rating;
        """
        
        data = [tuple(row) for row in df[['delivery_id', 'order_id', 'personnel_id', 'pickup_time',
                                        'estimated_delivery_time', 'actual_delivery_time', 
                                        'delivery_status', 'distance_km', 'delivery_rating', 
                                        'customer_feedback']].values]
        
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            logger.info(f"Loaded {len(data)} deliveries")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading deliveries: {e}")
    
    def load_inventory(self, df):
        """Load inventory into database"""
        logger.info("Loading inventory...")
        
        query = """
        INSERT INTO inventory (inventory_id, product_id, quantity_available, quantity_reserved,
                              reorder_level, last_restock_date, warehouse_location)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (inventory_id) DO UPDATE SET
            quantity_available = EXCLUDED.quantity_available,
            quantity_reserved = EXCLUDED.quantity_reserved,
            last_restock_date = EXCLUDED.last_restock_date;
        """
        
        data = [tuple(row) for row in df[['inventory_id', 'product_id', 'quantity_available',
                                          'quantity_reserved', 'reorder_level', 'last_restock_date',
                                          'warehouse_location']].values]
        
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            logger.info(f"Loaded {len(data)} inventory records")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading inventory: {e}")
    
    def run_pipeline(self, data_dir='data/processed'):
        """Run complete ETL pipeline"""
        logger.info("=" * 60)
        logger.info("Starting ETL Pipeline")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        self.connect_database()
        
        datasets = {
            'categories.csv': self.load_categories,
            'products.csv': self.load_products,
            'customers.csv': self.load_customers,
            'delivery_personnel.csv': self.load_delivery_personnel,
            'orders.csv': self.load_orders,
            'order_items.csv': self.load_order_items,
            'deliveries.csv': self.load_deliveries,
            'inventory.csv': self.load_inventory
        }
        
        for filename, load_func in datasets.items():
            file_path = f"{data_dir}/{filename.replace('.csv', '_clean.csv')}"
            
            if not pd.io.common.file_exists(file_path):
                file_path = f"data/raw/{filename}"
            
            df = self.extract_data(file_path)
            if df is None:
                continue
            
            df = self.transform_data(df, filename)
            
            load_func(df)
        
        self.close_connection()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info(f"ETL Pipeline completed in {duration:.2f} seconds")
        logger.info("=" * 60)

def main():
    """Main execution function"""
    pipeline = ETLPipeline()
    pipeline.run_pipeline()

if __name__ == "__main__":
    import os
    os.makedirs('logs', exist_ok=True)
    main()
