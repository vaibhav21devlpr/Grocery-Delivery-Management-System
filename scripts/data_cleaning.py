"""
Data Cleaning Module for Grocery Delivery System
Handles data validation, cleaning, and transformation
Author: Vaibhav Pandey
Date: April 2026
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataCleaner:
    """Class for cleaning and validating grocery delivery data"""
    
    def __init__(self):
        self.cleaning_stats = {
            'records_processed': 0,
            'records_cleaned': 0,
            'records_removed': 0,
            'null_values_handled': 0,
            'duplicates_removed': 0
        }
    
    def clean_customers(self, df):
        """Clean customer data"""
        logger.info(f"Cleaning customers data: {len(df)} records")
        initial_count = len(df)
        
        df = df.drop_duplicates(subset=['email'], keep='first')
        duplicates = initial_count - len(df)
        self.cleaning_stats['duplicates_removed'] += duplicates
        
        df['email'] = df['email'].str.lower().str.strip()
        df = df[df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$', na=False)]
        
        df['phone'] = df['phone'].astype(str).str.replace(r'[^0-9+]', '', regex=True)
        
        df['address'] = df['address'].fillna('Address not provided')
        df['city'] = df['city'].fillna('Unknown')
        df['state'] = df['state'].fillna('Unknown')
        
        df['is_active'] = df['is_active'].fillna(True).astype(bool)
        
        df['first_name'] = df['first_name'].str.title()
        df['last_name'] = df['last_name'].str.title()
        
        cleaned_count = initial_count - len(df) + duplicates
        self.cleaning_stats['records_removed'] += cleaned_count
        logger.info(f"Customers cleaned: {len(df)} records remaining, {cleaned_count} removed")
        
        return df
    
    def clean_products(self, df):
        """Clean product data"""
        logger.info(f"Cleaning products data: {len(df)} records")
        initial_count = len(df)
        
        df = df.drop_duplicates(subset=['product_name', 'category_id'], keep='first')
        
        df['product_name'] = df['product_name'].str.strip().str.title()
        
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df = df[df['price'] > 0]  # Remove invalid prices
        
        df['description'] = df['description'].fillna('No description available')
        
        df['is_active'] = df['is_active'].fillna(True).astype(bool)
        
        cleaned_count = initial_count - len(df)
        self.cleaning_stats['records_removed'] += cleaned_count
        logger.info(f"Products cleaned: {len(df)} records remaining")
        
        return df
    
    def clean_orders(self, df):
        """Clean orders data"""
        logger.info(f"Cleaning orders data: {len(df)} records")
        initial_count = len(df)
        
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df = df.dropna(subset=['order_date'])
        
        numeric_cols = ['total_amount', 'discount_amount', 'tax_amount', 'final_amount']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0)
        
        df = df[df['final_amount'] > 0]
        
        valid_payment_statuses = ['pending', 'completed', 'failed']
        df['payment_status'] = df['payment_status'].fillna('pending')
        df = df[df['payment_status'].isin(valid_payment_statuses)]
        
        valid_order_statuses = ['pending', 'confirmed', 'processing', 'out_for_delivery', 
                               'delivered', 'cancelled']
        df['order_status'] = df['order_status'].fillna('pending')
        df = df[df['order_status'].isin(valid_order_statuses)]
        
        df['delivery_address'] = df['delivery_address'].fillna('Address not provided')
        df['delivery_city'] = df['delivery_city'].fillna('Unknown')
        
        cleaned_count = initial_count - len(df)
        self.cleaning_stats['records_removed'] += cleaned_count
        logger.info(f"Orders cleaned: {len(df)} records remaining")
        
        return df
    
    def clean_order_items(self, df):
        """Clean order items data"""
        logger.info(f"Cleaning order items data: {len(df)} records")
        initial_count = len(df)
        
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df = df[df['quantity'] > 0]
        
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        df = df[df['unit_price'] > 0]
        
        df['subtotal'] = df['quantity'] * df['unit_price']
        df['subtotal'] = df['subtotal'].round(2)
        
        cleaned_count = initial_count - len(df)
        self.cleaning_stats['records_removed'] += cleaned_count
        logger.info(f"Order items cleaned: {len(df)} records remaining")
        
        return df
    
    def clean_deliveries(self, df):
        """Clean deliveries data"""
        logger.info(f"Cleaning deliveries data: {len(df)} records")
        initial_count = len(df)
        
        datetime_cols = ['pickup_time', 'estimated_delivery_time', 'actual_delivery_time']
        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        df['distance_km'] = pd.to_numeric(df['distance_km'], errors='coerce')
        df.loc[df['distance_km'] <= 0, 'distance_km'] = np.nan
        
        df['delivery_rating'] = pd.to_numeric(df['delivery_rating'], errors='coerce')
        df.loc[(df['delivery_rating'] < 0) | (df['delivery_rating'] > 5), 'delivery_rating'] = np.nan
        
        valid_statuses = ['pending', 'assigned', 'picked_up', 'in_transit', 'delivered', 'failed']
        df['delivery_status'] = df['delivery_status'].fillna('pending')
        df = df[df['delivery_status'].isin(valid_statuses)]
        
        cleaned_count = initial_count - len(df)
        self.cleaning_stats['records_removed'] += cleaned_count
        logger.info(f"Deliveries cleaned: {len(df)} records remaining")
        
        return df
    
    def clean_inventory(self, df):
        """Clean inventory data"""
        logger.info(f"Cleaning inventory data: {len(df)} records")
        initial_count = len(df)
        
        quantity_cols = ['quantity_available', 'quantity_reserved', 'reorder_level']
        for col in quantity_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0).astype(int)
            df.loc[df[col] < 0, col] = 0
        
        df['last_restock_date'] = pd.to_datetime(df['last_restock_date'], errors='coerce')
        
        df['warehouse_location'] = df['warehouse_location'].fillna('Warehouse-A')
        
        cleaned_count = initial_count - len(df)
        self.cleaning_stats['records_removed'] += cleaned_count
        logger.info(f"Inventory cleaned: {len(df)} records remaining")
        
        return df
    
    def detect_outliers(self, df, column, method='iqr', threshold=1.5):
        """Detect outliers using IQR or Z-score method"""
        if method == 'iqr':
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
        elif method == 'zscore':
            mean = df[column].mean()
            std = df[column].std()
            z_scores = np.abs((df[column] - mean) / std)
            outliers = df[z_scores > threshold]
        else:
            outliers = pd.DataFrame()
        
        if len(outliers) > 0:
            logger.warning(f"Found {len(outliers)} outliers in {column}")
        
        return outliers
    
    def validate_referential_integrity(self, orders_df, customers_df, order_items_df, products_df):
        """Validate foreign key relationships"""
        logger.info("Validating referential integrity...")
        
        valid_customer_ids = set(customers_df['customer_id'])
        invalid_orders = orders_df[~orders_df['customer_id'].isin(valid_customer_ids)]
        
        if len(invalid_orders) > 0:
            logger.warning(f"Found {len(invalid_orders)} orders with invalid customer_id")
            orders_df = orders_df[orders_df['customer_id'].isin(valid_customer_ids)]
        
        valid_order_ids = set(orders_df['order_id'])
        invalid_items = order_items_df[~order_items_df['order_id'].isin(valid_order_ids)]
        
        if len(invalid_items) > 0:
            logger.warning(f"Found {len(invalid_items)} order items with invalid order_id")
            order_items_df = order_items_df[order_items_df['order_id'].isin(valid_order_ids)]
        
        valid_product_ids = set(products_df['product_id'])
        invalid_items = order_items_df[~order_items_df['product_id'].isin(valid_product_ids)]
        
        if len(invalid_items) > 0:
            logger.warning(f"Found {len(invalid_items)} order items with invalid product_id")
            order_items_df = order_items_df[order_items_df['product_id'].isin(valid_product_ids)]
        
        logger.info("Referential integrity validation complete")
        
        return orders_df, order_items_df
    
    def get_data_quality_report(self, df, dataset_name):
        """Generate data quality report"""
        report = {
            'dataset': dataset_name,
            'total_records': len(df),
            'total_columns': len(df.columns),
            'null_counts': df.isnull().sum().to_dict(),
            'null_percentages': (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
            'duplicate_rows': df.duplicated().sum(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        logger.info(f"Data Quality Report for {dataset_name}:")
        logger.info(f"  Total Records: {report['total_records']}")
        logger.info(f"  Duplicate Rows: {report['duplicate_rows']}")
        logger.info(f"  Memory Usage: {report['memory_usage_mb']:.2f} MB")
        
        null_cols = {k: v for k, v in report['null_percentages'].items() if v > 0}
        if null_cols:
            logger.info(f"  Columns with nulls: {null_cols}")
        
        return report
    
    def print_stats(self):
        """Print cleaning statistics"""
        logger.info("=" * 60)
        logger.info("Data Cleaning Statistics:")
        for key, value in self.cleaning_stats.items():
            logger.info(f"  {key.replace('_', ' ').title()}: {value}")
        logger.info("=" * 60)
    
    def clean_delivery_personnel(self,df):
        df = pd.read_csv('data/raw/delivery_personnel.csv')
        
        df = df.drop_duplicates()
        df = df.dropna()

        df.to_csv('data/processed/delivery_personnel_clean.csv', index=False)
        print(f"Saved {len(df)} records to delivery_personnel_clean.csv")

        return df

def main():
    """Main function to clean all datasets"""
    cleaner = DataCleaner()
    
    logger.info("Loading raw data...")
    customers_df = pd.read_csv('data/raw/customers.csv')
    products_df = pd.read_csv('data/raw/products.csv')
    orders_df = pd.read_csv('data/raw/orders.csv')
    order_items_df = pd.read_csv('data/raw/order_items.csv')
    deliveries_df = pd.read_csv('data/raw/deliveries.csv')
    inventory_df = pd.read_csv('data/raw/inventory.csv')
    delivery_personnel_df = pd.read_csv('data/raw/delivery_personnel.csv')
    
    logger.info("\nStarting data cleaning process...")
    customers_clean = cleaner.clean_customers(customers_df)
    products_clean = cleaner.clean_products(products_df)
    orders_clean = cleaner.clean_orders(orders_df)
    order_items_clean = cleaner.clean_order_items(order_items_df)
    deliveries_clean = cleaner.clean_deliveries(deliveries_df)
    inventory_clean = cleaner.clean_inventory(inventory_df)
    delivery_personnel_clean = cleaner.clean_delivery_personnel(delivery_personnel_df)
    
    orders_clean, order_items_clean = cleaner.validate_referential_integrity(
        orders_clean, customers_clean, order_items_clean, products_clean
    )
    
    logger.info("\nSaving cleaned data...")
    customers_clean.to_csv('data/processed/customers_clean.csv', index=False)
    products_clean.to_csv('data/processed/products_clean.csv', index=False)
    orders_clean.to_csv('data/processed/orders_clean.csv', index=False)
    order_items_clean.to_csv('data/processed/order_items_clean.csv', index=False)
    deliveries_clean.to_csv('data/processed/deliveries_clean.csv', index=False)
    inventory_clean.to_csv('data/processed/inventory_clean.csv', index=False)
    delivery_personnel_clean.to_csv('data/processed/delivery_personnel_clean.csv', index=False)
    
    logger.info("\nGenerating data quality reports...")
    cleaner.get_data_quality_report(customers_clean, 'Customers')
    cleaner.get_data_quality_report(products_clean, 'Products')
    cleaner.get_data_quality_report(orders_clean, 'Orders')
    
    cleaner.print_stats()
    
    logger.info("\nData cleaning complete!")

if __name__ == "__main__":
    import os
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    main()
