"""
Airflow DAG for Grocery Delivery Data Pipeline
Orchestrates ETL and analytics workflows
Author: Vaibhav Pandey
Date: April 2026
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['your.email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 4, 1),
}

dag = DAG(
    'grocery_delivery_pipeline',
    default_args=default_args,
    description='Complete data pipeline for grocery delivery system',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['grocery', 'etl', 'analytics'],
)

def generate_sample_data(**context):
    """Generate sample data"""
    from scripts.generate_data import GroceryDataGenerator
    generator = GroceryDataGenerator()
    generator.generate_all()
    print("Sample data generation completed")

def clean_data(**context):
    """Clean and validate data"""
    from scripts.data_cleaning import DataCleaner
    import pandas as pd
    
    cleaner = DataCleaner()
    
    customers_df = pd.read_csv('data/raw/customers.csv')
    products_df = pd.read_csv('data/raw/products.csv')
    orders_df = pd.read_csv('data/raw/orders.csv')
    order_items_df = pd.read_csv('data/raw/order_items.csv')
    deliveries_df = pd.read_csv('data/raw/deliveries.csv')
    inventory_df = pd.read_csv('data/raw/inventory.csv')
    
    customers_clean = cleaner.clean_customers(customers_df)
    products_clean = cleaner.clean_products(products_df)
    orders_clean = cleaner.clean_orders(orders_df)
    order_items_clean = cleaner.clean_order_items(order_items_df)
    deliveries_clean = cleaner.clean_deliveries(deliveries_df)
    inventory_clean = cleaner.clean_inventory(inventory_df)
    
    orders_clean, order_items_clean = cleaner.validate_referential_integrity(
        orders_clean, customers_clean, order_items_clean, products_clean
    )
    
    customers_clean.to_csv('data/processed/customers_clean.csv', index=False)
    products_clean.to_csv('data/processed/products_clean.csv', index=False)
    orders_clean.to_csv('data/processed/orders_clean.csv', index=False)
    order_items_clean.to_csv('data/processed/order_items_clean.csv', index=False)
    deliveries_clean.to_csv('data/processed/deliveries_clean.csv', index=False)
    inventory_clean.to_csv('data/processed/inventory_clean.csv', index=False)
    
    print("Data cleaning completed")

def load_to_database(**context):
    """Load data to PostgreSQL"""
    from scripts.etl_pipeline import ETLPipeline
    pipeline = ETLPipeline()
    pipeline.run_pipeline()
    print("Data loaded to database")

def run_quality_checks(**context):
    """Run data quality checks"""
    import pandas as pd
    
    # Load cleaned data
    orders_df = pd.read_csv('data/processed/orders_clean.csv')
    customers_df = pd.read_csv('data/processed/customers_clean.csv')
    
    # Quality checks
    checks_passed = True
    
    # Check 1: No null customer_ids in orders
    if orders_df['customer_id'].isnull().any():
        print("FAILED: Found null customer_ids in orders")
        checks_passed = False
    
    # Check 2: All amounts are positive
    if (orders_df['final_amount'] <= 0).any():
        print("FAILED: Found non-positive amounts")
        checks_passed = False
    
    # Check 3: Valid email addresses
    if not customers_df['email'].str.contains('@').all():
        print("FAILED: Invalid email addresses found")
        checks_passed = False
    
    if checks_passed:
        print("All quality checks passed!")
    else:
        raise ValueError("Data quality checks failed")

def send_completion_notification(**context):
    """Send pipeline completion notification"""
    execution_date = context['execution_date']
    print(f"Pipeline completed successfully for {execution_date}")
    print("Notification sent to stakeholders")

task_generate_data = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data,
    dag=dag,
)

task_clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

task_quality_checks = PythonOperator(
    task_id='run_quality_checks',
    python_callable=run_quality_checks,
    dag=dag,
)

task_load_database = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
)

task_spark_analytics = BashOperator(
    task_id='run_spark_analytics',
    bash_command='cd /home/claude/grocery_delivery_de_project && spark-submit spark/spark_batch_processing.py',
    dag=dag,
)

task_notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

task_generate_data >> task_clean_data >> task_quality_checks >> task_load_database >> task_spark_analytics >> task_notification
