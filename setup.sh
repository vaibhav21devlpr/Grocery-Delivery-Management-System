#!/bin/bash
# Setup script for Grocery Delivery Management System
# Author: [Vaibhav Pandey]
# Date: April 2026

echo "=========================================="
echo "Grocery Delivery System - Setup Script"
echo "=========================================="

# Create necessary directories
echo "Creating project directories..."
mkdir -p data/raw data/processed data/streaming logs

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Setup PostgreSQL database
echo "Setting up PostgreSQL database..."
read -p "Do you want to create the database? (y/n): " create_db

if [ "$create_db" == "y" ]; then
    echo "Creating database 'grocery_delivery'..."
    psql -U postgres -c "CREATE DATABASE grocery_delivery;"
    
    echo "Creating database schema..."
    psql -U postgres -d grocery_delivery -f sql/schema.sql
    
    echo "Creating data warehouse schema..."
    psql -U postgres -d grocery_delivery -f sql/star_schema.sql
    
    echo "Database setup complete!"
fi

# Generate sample data
echo "Generating sample data..."
python3 scripts/generate_data.py

# Clean data
echo "Cleaning data..."
python3 scripts/data_cleaning.py

# Setup Kafka (optional)
read -p "Do you want to setup Kafka? (y/n): " setup_kafka

if [ "$setup_kafka" == "y" ]; then
    echo "Creating Kafka topics..."
    kafka-topics.sh --create --topic grocery-orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    kafka-topics.sh --create --topic grocery-deliveries --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    echo "Kafka topics created!"
fi

# Setup Airflow (optional)
read -p "Do you want to setup Airflow? (y/n): " setup_airflow

if [ "$setup_airflow" == "y" ]; then
    echo "Initializing Airflow database..."
    airflow db init
    
    echo "Creating Airflow admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    
    echo "Airflow setup complete!"
    echo "Start Airflow with:"
    echo "  airflow webserver -p 8080"
    echo "  airflow scheduler"
fi

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Update config/database.yaml with your database credentials"
echo "2. Run ETL pipeline: python3 scripts/etl_pipeline.py"
echo "3. Run Spark analytics: spark-submit spark/spark_batch_processing.py"
echo "4. Start Kafka producer: python3 kafka/producer.py"
echo "5. Start Kafka consumer: python3 kafka/consumer.py"
echo ""
echo "For more information, see README.md"
echo "=========================================="
