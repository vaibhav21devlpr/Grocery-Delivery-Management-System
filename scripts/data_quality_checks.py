"""
Data Quality Checks for Grocery Delivery System
Validates data integrity and business rules
Author: Vaibhav Pandey
Date: April 2026
"""

import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    """Performs comprehensive data quality checks"""
    
    def __init__(self):
        self.results = {
            'total_checks': 0,
            'passed': 0,
            'failed': 0,
            'warnings': 0
        }
    
    def check_null_values(self, df, dataset_name, critical_columns):
        """Check for null values in critical columns"""
        logger.info(f"\nChecking null values in {dataset_name}...")
        
        for col in critical_columns:
            null_count = df[col].isnull().sum()
            self.results['total_checks'] += 1
            
            if null_count > 0:
                logger.error(f"  ❌ FAILED: Found {null_count} null values in {col}")
                self.results['failed'] += 1
            else:
                logger.info(f"  ✓ PASSED: No null values in {col}")
                self.results['passed'] += 1
    
    def check_data_types(self, df, dataset_name, type_rules):
        """Validate data types"""
        logger.info(f"\nChecking data types in {dataset_name}...")
        
        for col, expected_type in type_rules.items():
            self.results['total_checks'] += 1
            actual_type = df[col].dtype
            
            if str(actual_type).startswith(expected_type):
                logger.info(f"  ✓ PASSED: {col} is {actual_type}")
                self.results['passed'] += 1
            else:
                logger.error(f"  ❌ FAILED: {col} is {actual_type}, expected {expected_type}")
                self.results['failed'] += 1
    
    def check_duplicates(self, df, dataset_name, unique_columns):
        """Check for duplicate records"""
        logger.info(f"\nChecking duplicates in {dataset_name}...")
        
        self.results['total_checks'] += 1
        duplicates = df.duplicated(subset=unique_columns).sum()
        
        if duplicates > 0:
            logger.error(f"  ❌ FAILED: Found {duplicates} duplicate records")
            self.results['failed'] += 1
        else:
            logger.info(f"  ✓ PASSED: No duplicate records found")
            self.results['passed'] += 1
    
    def check_value_ranges(self, df, dataset_name, range_rules):
        """Validate value ranges"""
        logger.info(f"\nChecking value ranges in {dataset_name}...")
        
        for col, (min_val, max_val) in range_rules.items():
            self.results['total_checks'] += 1
            
            invalid = df[(df[col] < min_val) | (df[col] > max_val)]
            
            if len(invalid) > 0:
                logger.error(f"  ❌ FAILED: Found {len(invalid)} values outside range [{min_val}, {max_val}] in {col}")
                self.results['failed'] += 1
            else:
                logger.info(f"  ✓ PASSED: All {col} values within range [{min_val}, {max_val}]")
                self.results['passed'] += 1
    
    def check_referential_integrity(self, child_df, parent_df, foreign_key, primary_key, dataset_name):
        """Check foreign key relationships"""
        logger.info(f"\nChecking referential integrity for {dataset_name}...")
        
        self.results['total_checks'] += 1
        
        parent_ids = set(parent_df[primary_key])
        child_ids = set(child_df[foreign_key])
        orphans = child_ids - parent_ids
        
        if orphans:
            logger.error(f"  ❌ FAILED: Found {len(orphans)} orphan records")
            self.results['failed'] += 1
        else:
            logger.info(f"  ✓ PASSED: All foreign keys valid")
            self.results['passed'] += 1
    
    def check_business_rules(self, df, dataset_name, rules):
        """Validate business rules"""
        logger.info(f"\nChecking business rules in {dataset_name}...")
        
        for rule_name, condition in rules.items():
            self.results['total_checks'] += 1
            
            violations = df[~condition(df)]
            
            if len(violations) > 0:
                logger.error(f"  ❌ FAILED: {rule_name} - {len(violations)} violations")
                self.results['failed'] += 1
            else:
                logger.info(f"  ✓ PASSED: {rule_name}")
                self.results['passed'] += 1
    
    def print_summary(self):
        """Print quality check summary"""
        logger.info("\n" + "=" * 60)
        logger.info("DATA QUALITY CHECK SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Checks: {self.results['total_checks']}")
        logger.info(f"Passed: {self.results['passed']}")
        logger.info(f"Failed: {self.results['failed']}")
        logger.info(f"Warnings: {self.results['warnings']}")
        
        if self.results['failed'] == 0:
            logger.info("\n✅ ALL CHECKS PASSED!")
        else:
            logger.warning(f"\n⚠️  {self.results['failed']} CHECKS FAILED")
        
        logger.info("=" * 60)

def main():
    """Run all data quality checks"""
    logger.info("Starting Data Quality Checks...")
    logger.info("=" * 60)
    
    checker = DataQualityChecker()
    
    customers_df = pd.read_csv('data/processed/customers_clean.csv')
    products_df = pd.read_csv('data/processed/products_clean.csv')
    orders_df = pd.read_csv('data/processed/orders_clean.csv')
    order_items_df = pd.read_csv('data/processed/order_items_clean.csv')
    
    checker.check_null_values(
        customers_df, 
        'Customers',
        ['customer_id', 'email', 'first_name', 'last_name']
    )
    
    checker.check_null_values(
        orders_df,
        'Orders',
        ['order_id', 'customer_id', 'final_amount']
    )
    
    checker.check_data_types(
        orders_df,
        'Orders',
        {'order_id': 'int', 'final_amount': 'float'}
    )
    
    checker.check_duplicates(
        customers_df,
        'Customers',
        ['email']
    )
    
    checker.check_duplicates(
        orders_df,
        'Orders',
        ['order_id']
    )
    
    checker.check_value_ranges(
        products_df,
        'Products',
        {'price': (0, 10000)}
    )
    
    checker.check_value_ranges(
        orders_df,
        'Orders',
        {'final_amount': (0, 1000000)}
    )
    
    checker.check_referential_integrity(
        orders_df,
        customers_df,
        'customer_id',
        'customer_id',
        'Orders -> Customers'
    )
    
    checker.check_referential_integrity(
        order_items_df,
        products_df,
        'product_id',
        'product_id',
        'Order Items -> Products'
    )
    
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
    
    business_rules = {
        'Order amounts are positive': lambda df: df['final_amount'] > 0,
        'Order dates are valid': lambda df: df['order_date'] <= pd.Timestamp.now(),
        'Tax is calculated correctly (5%)': lambda df: abs((df['total_amount'] - df['discount_amount']) * 0.05 - df['tax_amount']) < 1,
    }
    
    checker.check_business_rules(orders_df, 'Orders', business_rules)
    
    checker.print_summary()

if __name__ == "__main__":
    main()
