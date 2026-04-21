"""
Spark Batch Processing for Grocery Delivery System
Performs large-scale data analytics using PySpark
Author: Vaibhav Pandey
Date: April 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import sys
import os

class GrocerySparkProcessor:
    """Spark processor for batch analytics"""
    
    def __init__(self, app_name="GroceryDeliveryAnalytics"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"Spark Session initialized: {app_name}")
    
    def load_data(self, file_path, file_format="csv"):
        """Load data from file"""
        try:
            df = self.spark.read \
                .format(file_format) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(file_path)
            
            print(f"Loaded {df.count()} records from {file_path}")
            return df
        except Exception as e:
            print(f"Error loading data: {e}")
            return None
    
    def analyze_sales_performance(self, orders_df, order_items_df, products_df):
        """Analyze sales performance by product"""
        print("\n=== Sales Performance Analysis ===")
        
        sales_df = order_items_df.join(products_df, "product_id") \
                                 .join(orders_df, "order_id")
        
        product_sales = sales_df.groupBy("product_id", "product_name", "category_id") \
            .agg(
                count("order_item_id").alias("times_ordered"),
                sum("quantity").alias("total_quantity_sold"),
                sum("subtotal").alias("total_revenue"),
                avg("unit_price").alias("avg_price"),
                countDistinct("customer_id").alias("unique_customers")
            ) \
            .orderBy(col("total_revenue").desc())
        
        print("Top 10 Products by Revenue:")
        product_sales.show(10, truncate=False)
        
        return product_sales
    
    def analyze_customer_behavior(self, customers_df, orders_df):
        """Analyze customer behavior patterns"""
        print("\n=== Customer Behavior Analysis ===")
        
        customer_metrics = orders_df.groupBy("customer_id") \
            .agg(
                count("order_id").alias("total_orders"),
                sum("final_amount").alias("lifetime_value"),
                avg("final_amount").alias("avg_order_value"),
                max("order_date").alias("last_order_date"),
                datediff(current_date(), max("order_date")).alias("days_since_last_order")
            )
        
        customer_analysis = customers_df.join(customer_metrics, "customer_id")
        
        customer_analysis = customer_analysis.withColumn(
            "customer_tier",
            when(col("lifetime_value") > 50000, "Premium")
            .when(col("lifetime_value") > 20000, "Gold")
            .when(col("lifetime_value") > 10000, "Silver")
            .otherwise("Bronze")
        )
        
        print("Customer Segmentation:")
        customer_analysis.groupBy("customer_tier") \
            .agg(
                count("customer_id").alias("customer_count"),
                avg("lifetime_value").alias("avg_lifetime_value"),
                avg("total_orders").alias("avg_orders")
            ) \
            .orderBy(col("avg_lifetime_value").desc()) \
            .show()
        
        return customer_analysis
    
    def analyze_delivery_performance(self, deliveries_df, delivery_personnel_df):
        """Analyze delivery performance metrics"""
        print("\n=== Delivery Performance Analysis ===")
        
        deliveries_analysis = deliveries_df.withColumn(
            "delivery_time_minutes",
            (unix_timestamp("actual_delivery_time") - 
             unix_timestamp("pickup_time")) / 60
        ).withColumn(
            "is_on_time",
            when(col("actual_delivery_time") <= col("estimated_delivery_time"), 1).otherwise(0)
        )
        
        personnel_performance = deliveries_analysis.groupBy("personnel_id") \
            .agg(
                count("delivery_id").alias("total_deliveries"),
                avg("delivery_time_minutes").alias("avg_delivery_minutes"),
                avg("delivery_rating").alias("avg_rating"),
                (sum("is_on_time") / count("delivery_id") * 100).alias("on_time_percentage"),
                avg("distance_km").alias("avg_distance_km")
            )
        
        dp = delivery_personnel_df.alias("dp")
        pp = personnel_performance.alias("pp")

        performance_report = dp.join(
            pp, col("dp.personnel_id") == col("pp.personnel_id")
        ).orderBy(col("pp.on_time_percentage").desc())
        
        print("Top 10 Delivery Personnel by On-Time Percentage:")
        performance_report_clean = performance_report.select(
            col("dp.personnel_id").alias("personnel_id"),
            concat(col("dp.first_name"), lit(" "), col("dp.last_name")).alias("name"),
            col("dp.vehicle_type"),
            col("pp.total_deliveries"),
            round(col("pp.avg_delivery_minutes"), 2).alias("avg_delivery_mins"),
            round(col("pp.avg_rating"), 2).alias("rating"),
            round(col("pp.on_time_percentage"), 2).alias("on_time_%")
        )

        return performance_report_clean
    
    def analyze_time_patterns(self, orders_df):
        """Analyze temporal ordering patterns"""
        print("\n=== Temporal Pattern Analysis ===")
        
        time_analysis = orders_df.withColumn("hour", hour("order_date")) \
                                 .withColumn("day_of_week", dayofweek("order_date")) \
                                 .withColumn("month", month("order_date"))
        
        peak_hours = time_analysis.groupBy("hour") \
            .agg(
                count("order_id").alias("order_count"),
                sum("final_amount").alias("revenue")
            ) \
            .orderBy(col("order_count").desc())
        
        print("Peak Order Hours:")
        peak_hours.show(10)
        
        dow_patterns = time_analysis.groupBy("day_of_week") \
            .agg(
                count("order_id").alias("order_count"),
                avg("final_amount").alias("avg_order_value")
            ) \
            .orderBy("day_of_week")
        
        print("\nOrders by Day of Week:")
        dow_patterns.show()
        
        return time_analysis
    
    def detect_anomalies(self, orders_df):
        """Detect anomalous orders"""
        print("\n=== Anomaly Detection ===")
        
        stats = orders_df.select(
            mean("final_amount").alias("mean"),
            stddev("final_amount").alias("stddev")
        ).collect()[0]
        
        anomalies = orders_df.filter(
            (col("final_amount") > stats["mean"] + 3 * stats["stddev"]) |
            (col("final_amount") < stats["mean"] - 3 * stats["stddev"])
        )
        
        print(f"Found {anomalies.count()} anomalous orders")
        if anomalies.count() > 0:
            anomalies.select("order_id", "customer_id", "final_amount", "order_date") \
                     .show(10, truncate=False)
        
        return anomalies
    
    def calculate_inventory_metrics(self, inventory_df, order_items_df, products_df):
        """Calculate inventory turnover and stock metrics"""
        print("\n=== Inventory Analysis ===")
        
        recent_sales = order_items_df.groupBy("product_id") \
            .agg(sum("quantity").alias("total_sold"))
        
        inventory_analysis = inventory_df.join(products_df, "product_id") \
            .join(recent_sales, "product_id", "left") \
            .na.fill({"total_sold": 0})
        
        inventory_analysis = inventory_analysis.withColumn(
            "turnover_rate",
            when(col("quantity_available") > 0, 
                 col("total_sold") / col("quantity_available")).otherwise(0)
        ).withColumn(
            "stock_status",
            when(col("quantity_available") <= 0, "OUT_OF_STOCK")
            .when(col("quantity_available") <= col("reorder_level"), "LOW_STOCK")
            .otherwise("SUFFICIENT")
        )
        
        print("Inventory Stock Status:")
        inventory_analysis.groupBy("stock_status") \
            .agg(count("product_id").alias("product_count")) \
            .show()
        
        print("\nLow Stock Products:")
        inventory_analysis.filter(col("stock_status") == "LOW_STOCK") \
            .select("product_name", "quantity_available", "reorder_level") \
            .show(10, truncate=False)
        
        return inventory_analysis
    
    def generate_cohort_analysis(self, customers_df, orders_df):
        """Perform cohort analysis"""
        print("\n=== Cohort Analysis ===")
        
        customer_cohorts = customers_df.withColumn(
            "cohort_month",
            date_format("registration_date", "yyyy-MM")
        )
        
        cohort_orders = customer_cohorts.join(orders_df, "customer_id")
        
        cohort_metrics = cohort_orders.groupBy("cohort_month") \
            .agg(
                countDistinct("customer_id").alias("total_customers"),
                count("order_id").alias("total_orders"),
                sum("final_amount").alias("total_revenue"),
                avg("final_amount").alias("avg_order_value")
            ) \
            .orderBy("cohort_month")
        
        print("Monthly Cohort Performance:")
        cohort_metrics.show(truncate=False)
        
        return cohort_metrics
    
    def save_results(self, df, output_path):
        """Save analysis results"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)  # ✅ ensure folder exists
            
            pdf = df.toPandas()
            pdf.to_csv(output_path + ".csv", index=False)
            
            print(f"Results saved to {output_path}.csv")
        except Exception as e:
            print(f"Error saving results: {e}")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        print("Spark session stopped")

def main():
    """Main execution function"""
    print("=" * 70)
    print("Grocery Delivery Analytics - Spark Batch Processing")
    print("=" * 70)
    
    processor = GrocerySparkProcessor()
    
    print("\nLoading datasets...")
    customers_df = processor.load_data("data/processed/customers_clean.csv")
    products_df = processor.load_data("data/processed/products_clean.csv")
    orders_df = processor.load_data("data/processed/orders_clean.csv")
    order_items_df = processor.load_data("data/processed/order_items_clean.csv")
    deliveries_df = processor.load_data("data/processed/deliveries_clean.csv")
    delivery_personnel_df = processor.load_data("data/processed/delivery_personnel_clean.csv")
    inventory_df = processor.load_data("data/processed/inventory_clean.csv")
    
    if not all([customers_df, products_df, orders_df, order_items_df]):
        print("Error: Required datasets not loaded")
        processor.stop()
        sys.exit(1)
    
    sales_analysis = processor.analyze_sales_performance(orders_df, order_items_df, products_df)
    customer_analysis = processor.analyze_customer_behavior(customers_df, orders_df)
    delivery_analysis = processor.analyze_delivery_performance(deliveries_df, delivery_personnel_df)
    time_patterns = processor.analyze_time_patterns(orders_df)
    anomalies = processor.detect_anomalies(orders_df)
    inventory_metrics = processor.calculate_inventory_metrics(inventory_df, order_items_df, products_df)
    cohort_analysis = processor.generate_cohort_analysis(customers_df, orders_df)
    
    print("\nSaving analysis results...")
    processor.save_results(sales_analysis, "data/output/sales_analysis")
    processor.save_results(customer_analysis, "data/output/customer_analysis")
    processor.save_results(delivery_analysis, "data/output/delivery_analysis")
    
    processor.stop()
    
    print("\n" + "=" * 70)
    print("Analysis Complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()
