"""
Kafka Consumer for Grocery Delivery System
Consumes and processes order events
Author: Vaibhav Pandey
Date: April 2026
"""

from kafka import KafkaConsumer
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GroceryOrderConsumer:
    """Kafka consumer for grocery orders"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='grocery-orders', group_id='grocery-consumer-group'):
        self.topic = topic
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info(f"Kafka consumer initialized for topic: {topic}")
            self.order_count = 0
            self.total_revenue = 0
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def process_order(self, order):
        """Process individual order"""
        self.order_count += 1
        self.total_revenue += order['total_amount']
        
        logger.info(f"Processing Order #{order['order_id']}")
        logger.info(f"  Customer ID: {order['customer_id']}")
        logger.info(f"  Items: {len(order['items'])}")
        logger.info(f"  Total Amount: ₹{order['total_amount']:.2f}")
        logger.info(f"  Payment Method: {order['payment_method']}")
        logger.info(f"  Delivery City: {order['delivery_address']['city']}")
        
        if order['total_amount'] > 5000:
            logger.warning(f"  High-value order detected: ₹{order['total_amount']:.2f}")
        
        if len(order['items']) > 10:
            logger.info(f"  Bulk order: {len(order['items'])} items")
        
        logger.info("-" * 50)
    
    def consume_orders(self):
        """Consume orders from Kafka topic"""
        logger.info("Starting to consume orders...")
        logger.info(f"Listening on topic: {self.topic}")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)
        
        try:
            for message in self.consumer:
                order = message.value
                self.process_order(order)
                
                if self.order_count % 10 == 0:
                    self.print_statistics()
        
        except KeyboardInterrupt:
            logger.info("\nConsumer interrupted by user")
        finally:
            self.close()
    
    def print_statistics(self):
        """Print current statistics"""
        avg_order_value = self.total_revenue / self.order_count if self.order_count > 0 else 0
        
        logger.info("=" * 60)
        logger.info("STATISTICS")
        logger.info(f"Total Orders Processed: {self.order_count}")
        logger.info(f"Total Revenue: ₹{self.total_revenue:.2f}")
        logger.info(f"Average Order Value: ₹{avg_order_value:.2f}")
        logger.info("=" * 60)
    
    def close(self):
        """Close Kafka consumer"""
        self.print_statistics()
        self.consumer.close()
        logger.info("Kafka consumer closed")

def main():
    """Main execution function"""
    print("=" * 60)
    print("Grocery Order Stream - Kafka Consumer")
    print("=" * 60)
    
    consumer = GroceryOrderConsumer()
    consumer.consume_orders()

if __name__ == "__main__":
    main()
