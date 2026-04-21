# Quick Start Guide - Grocery Delivery Management System

## Project Overview
This Data Engineering project demonstrates a complete end-to-end data pipeline for a Grocery Delivery Management System, incorporating batch processing, real-time streaming, data warehousing, and orchestration.

## Key Features

### 1. Data Architecture
- **Relational Database (PostgreSQL)**: OLTP system with normalized schema
- **Data Warehouse**: Star schema for analytics (OLAP)
- **Distributed Storage**: HDFS/S3 for big data
- **Streaming Platform**: Apache Kafka for real-time events

### 2. Data Pipeline Components
- **Batch Ingestion**: CSV to PostgreSQL ETL pipeline
- **Real-time Streaming**: Kafka-based order event processing
- **Big Data Processing**: PySpark for large-scale analytics
- **Workflow Orchestration**: Apache Airflow DAGs

### 3. Analytics & Insights
- Sales performance analysis
- Customer segmentation (RFM analysis)
- Delivery performance metrics
- Inventory optimization
- Demand forecasting
- Cohort analysis

## Quick Setup (5 Minutes)

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Generate Sample Data
```bash
python scripts/generate_data.py
```
This creates 8 CSV files with realistic sample data:
- 1,000 customers
- 200 products
- 5,000 orders
- Order items, deliveries, inventory

### Step 3: Clean Data
```bash
python scripts/data_cleaning.py
```
Performs data validation, cleaning, and quality checks.

### Step 4: Run Analytics
```bash
python spark/spark_batch_processing.py
```
Or if you have Spark installed:
```bash
spark-submit spark/spark_batch_processing.py
```

## Database Setup (Optional)

### Create PostgreSQL Database
```bash
createdb grocery_delivery
psql -d grocery_delivery -f sql/schema.sql
psql -d grocery_delivery -f sql/star_schema.sql
```

### Load Data to Database
```bash
python scripts/etl_pipeline.py
```

### Run Analytical Queries
```bash
psql -d grocery_delivery -f sql/queries.sql
```

## Real-time Streaming Demo

### Terminal 1: Start Kafka Producer
```bash
python kafka/producer.py
```

### Terminal 2: Start Kafka Consumer
```bash
python kafka/consumer.py
```

## Airflow Orchestration

### Initialize Airflow
```bash
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@example.com
```

### Start Airflow Services
```bash
# Terminal 1
airflow webserver -p 8080

# Terminal 2
airflow scheduler
```

### Access Airflow UI
Open browser: http://localhost:8080
Username: admin, Password: admin

### Trigger DAG
Enable and trigger the `grocery_delivery_pipeline` DAG

## Project Structure Explained

```
grocery_delivery_de_project/
│
├── data/                      # Data storage
│   ├── raw/                   # Raw CSV files (generated)
│   └── processed/             # Cleaned data
│
├── sql/                       # Database scripts
│   ├── schema.sql             # OLTP schema
│   ├── star_schema.sql        # OLAP schema
│   └── queries.sql            # Analytics queries
│
├── scripts/                   # Python ETL scripts
│   ├── generate_data.py       # Data generator
│   ├── data_cleaning.py       # Data cleaning
│   └── etl_pipeline.py        # ETL pipeline
│
├── spark/                     # PySpark jobs
│   └── spark_batch_processing.py
│
├── kafka/                     # Streaming components
│   ├── producer.py            # Event producer
│   └── consumer.py            # Event consumer
│
├── airflow/dags/              # Workflow orchestration
│   └── grocery_pipeline_dag.py
│
└── config/                    # Configuration files
    ├── database.yaml
    └── kafka.properties
```

## Key Technologies Demonstrated

1. **Linux & File Systems**: Directory structure, file permissions, automation
2. **Python Programming**: Pandas, NumPy, object-oriented design
3. **SQL**: Complex queries, joins, window functions, CTEs
4. **Database Design**: Normalization, star schema, indexing
5. **ETL/ELT**: Data extraction, transformation, loading
6. **Big Data**: Apache Spark, distributed processing
7. **Streaming**: Apache Kafka, real-time event processing
8. **Orchestration**: Apache Airflow, DAGs, scheduling
9. **Data Quality**: Validation, cleaning, anomaly detection

## Sample Outputs

### Data Generation Output
```
✓ Generated categories.csv with 10 records
✓ Generated products.csv with 200 records
✓ Generated customers.csv with 1000 records
✓ Generated orders.csv with 5000 records
✓ Generated deliveries.csv with 4000 records
```

### Spark Analytics Output
```
=== Sales Performance Analysis ===
Top 10 Products by Revenue:
+----------+--------------+-----------+--------------+------------------+
|product_id|product_name  |times_ordered|total_revenue|unique_customers|
+----------+--------------+-----------+--------------+------------------+
|45        |Chicken       |892        |267600.00     |654              |
|101       |Rice          |856        |171200.00     |612              |
+----------+--------------+-----------+--------------+------------------+
```

### Data Quality Report
```
Data Quality Report for Orders:
  Total Records: 5000
  Duplicate Rows: 0
  Memory Usage: 2.34 MB
  All quality checks passed!
```

## Common Tasks

### Update Configuration
Edit `config/database.yaml` with your database credentials:
```yaml
database:
  host: localhost
  port: 5432
  database: grocery_delivery
  user: your_username
  password: your_password
```

### Run Custom Analytics
Modify `sql/queries.sql` or create new queries:
```sql
SELECT 
    product_name,
    SUM(quantity) as total_sold
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY product_name
ORDER BY total_sold DESC;
```

### Add New Data Sources
Extend `generate_data.py` to include:
- Customer reviews
- Product ratings
- Delivery routes
- Promotional campaigns

## Performance Optimization Tips

1. **Database Indexing**: Already created on foreign keys
2. **Spark Partitioning**: Use `.repartition()` for large datasets
3. **Batch Size**: Adjust in `etl_pipeline.py` for optimal loading
4. **Kafka Partitions**: Increase for higher throughput
5. **Airflow Parallelism**: Configure concurrent task execution

## Troubleshooting

### Issue: Kafka Connection Failed
**Solution**: Ensure Kafka and Zookeeper are running
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

### Issue: Database Connection Error
**Solution**: Check PostgreSQL service and credentials
```bash
sudo systemctl status postgresql
psql -U postgres -c "SELECT version();"
```

### Issue: Spark Out of Memory
**Solution**: Increase executor memory in spark-submit
```bash
spark-submit --executor-memory 4g --driver-memory 2g spark/spark_batch_processing.py
```

## Next Steps for Enhancement

1. **Machine Learning**: Add demand forecasting models
2. **Cloud Deployment**: Migrate to AWS/GCP/Azure
3. **Real-time Dashboard**: Build with Streamlit/Dash
4. **API Layer**: Create REST API with FastAPI
5. **Data Lineage**: Implement tracking with Apache Atlas
6. **CI/CD**: Add automated testing and deployment

## Documentation Files

- `README.md`: Complete project documentation
- `QUICKSTART.md`: This file

## Support & Resources

- Apache Spark: https://spark.apache.org/docs/
- Apache Kafka: https://kafka.apache.org/documentation/
- Apache Airflow: https://airflow.apache.org/docs/
- PostgreSQL: https://www.postgresql.org/docs/

## Project Submission Checklist

✅ Complete working project
✅ GitHub repository ready
✅ Documentation (PDF) created
✅ All required files included
✅ Code is well-commented
✅ Demonstrates data engineering skills
✅ Follows project guidelines

---

**Author**: Vaibhav Pandey 
**Roll Number**: 2306078
**Batch/Program**: Data Engineering  
**Date**: April 2026  
**Project**: Grocery Delivery Management System
