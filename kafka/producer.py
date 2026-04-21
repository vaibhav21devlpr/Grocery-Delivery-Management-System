"""
Kafka Producer for Grocery Delivery System
Simulates real-time order events
Author: Vaibhav Pandey
Date: April 2026
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GroceryOrderProducer:
    """Kafka producer for grocery orders"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='grocery-orders'):
        self.topic = topic
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Kafka producer initialized for topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def generate_order_event(self, order_id):
        """Generate a random order event"""
        products = [
            {"name": "Apple", "price": 150.00},
            {"name": "Banana", "price": 50.00},
            {"name": "Milk", "price": 60.00},
            {"name": "Bread", "price": 40.00},
            {"name": "Eggs", "price": 80.00},
            {"name": "Rice", "price": 200.00},
            {"name": "Chicken", "price": 300.00},
            {"name": "Tomato", "price": 40.00}
        ]
        
        num_items = random.randint(1, 5)
        items = []
        total = 0
        
        for _ in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 3)
            subtotal = product["price"] * quantity
            total += subtotal
            
            items.append({
                "product_name": product["name"],
                "quantity": quantity,
                "unit_price": product["price"],
                "subtotal": subtotal
            })
        
        order = {
            "order_id": order_id,
            "customer_id": random.randint(1, 1000),
            "timestamp": datetime.now().isoformat(),
            "items": items,
            "total_amount": total,
            "payment_method": random.choice(["UPI", "Credit Card", "Cash on Delivery"]),
            "delivery_address": {
                "city": random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai"]),
                "zip": str(random.randint(100000, 999999))
            },
            "status": "pending"
        }
        
        return order
    
    def send_order(self, order):
        """Send order event to Kafka"""
        try:
            future = self.producer.send(self.topic, value=order)
            metadata = future.get(timeout=10)
            logger.info(f"Order {order['order_id']} sent to partition {metadata.partition} at offset {metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to send order {order['order_id']}: {e}")
            return False
    
    def simulate_order_stream(self, num_orders=100, delay=1):
        """Simulate continuous stream of orders"""
        logger.info(f"Starting order stream simulation: {num_orders} orders")
        
        for i in range(1, num_orders + 1):
            order = self.generate_order_event(i)
            self.send_order(order)
            time.sleep(delay)
        
        logger.info(f"Completed sending {num_orders} orders")
    
    def close(self):
        """Close Kafka producer"""
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")

def main():
    """Main execution function"""
    print("=" * 60)
    print("Grocery Order Stream - Kafka Producer")
    print("=" * 60)
    
    producer = GroceryOrderProducer()
    
    try:
        producer.simulate_order_stream(num_orders=50, delay=2)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    finally:
        producer.close()
    
    print("=" * 60)
    print("Producer Stopped")
    print("=" * 60)

if __name__ == "__main__":
    main()
