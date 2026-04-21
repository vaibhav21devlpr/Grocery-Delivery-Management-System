"""
Data Generator for Grocery Delivery Management System
Generates realistic sample data for testing and development
Author: Vaibhav Pandey
Date: April 2026
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import csv
import os

np.random.seed(42)
random.seed(42)

class GroceryDataGenerator:
    def __init__(self, output_dir='data/raw'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.num_customers = 1000
        self.num_products = 200
        self.num_orders = 5000
        self.num_delivery_personnel = 50
        
        self.categories_data = []
        self.products_data = []
        self.customers_data = []
        self.delivery_personnel_data = []
        self.orders_data = []
        self.order_items_data = []
        self.deliveries_data = []
        self.inventory_data = []
        
    def generate_categories(self):
        """Generate product categories"""
        categories = [
            ('Fruits & Vegetables', 'Fresh fruits and vegetables'),
            ('Dairy & Eggs', 'Milk, cheese, yogurt, and eggs'),
            ('Bakery', 'Bread, cakes, and pastries'),
            ('Beverages', 'Drinks, juices, and soft drinks'),
            ('Snacks & Packaged Foods', 'Chips, cookies, and ready-to-eat meals'),
            ('Personal Care', 'Soaps, shampoos, and hygiene products'),
            ('Household Items', 'Cleaning supplies and home essentials'),
            ('Frozen Foods', 'Ice cream, frozen vegetables, and meals'),
            ('Meat & Seafood', 'Fresh and frozen meats'),
            ('Cereals & Breakfast', 'Cereals, oats, and breakfast items')
        ]
        
        for idx, (name, desc) in enumerate(categories, 1):
            self.categories_data.append({
                'category_id': idx,
                'category_name': name,
                'description': desc
            })
        
        return self.categories_data
    
    def generate_products(self):
        """Generate product catalog"""
        product_names = {
            1: ['Apple', 'Banana', 'Orange', 'Mango', 'Grapes', 'Tomato', 'Potato', 'Onion', 
                'Carrot', 'Spinach', 'Broccoli', 'Cucumber', 'Watermelon', 'Papaya', 'Lemon'],
            2: ['Milk', 'Cheese', 'Yogurt', 'Butter', 'Eggs', 'Paneer', 'Cream', 'Curd'],
            3: ['White Bread', 'Brown Bread', 'Cake', 'Cookies', 'Croissant', 'Muffin'],
            4: ['Coke', 'Pepsi', 'Orange Juice', 'Apple Juice', 'Mineral Water', 'Energy Drink', 'Tea', 'Coffee'],
            5: ['Chips', 'Biscuits', 'Namkeen', 'Instant Noodles', 'Pasta', 'Rice'],
            6: ['Shampoo', 'Soap', 'Toothpaste', 'Face Wash', 'Hand Sanitizer', 'Lotion'],
            7: ['Detergent', 'Dish Soap', 'Floor Cleaner', 'Toilet Cleaner', 'Napkins'],
            8: ['Ice Cream', 'Frozen Peas', 'Frozen Pizza', 'Frozen Paratha'],
            9: ['Chicken', 'Fish', 'Mutton', 'Prawns', 'Eggs (Meat Section)'],
            10: ['Corn Flakes', 'Oats', 'Muesli', 'Bread', 'Jam', 'Honey']
        }
        
        units = ['kg', 'ltr', 'pcs', 'pack', 'box', 'bottle']
        
        product_id = 1
        for category_id, names in product_names.items():
            for name in names:
                self.products_data.append({
                    'product_id': product_id,
                    'product_name': name,
                    'category_id': category_id,
                    'price': round(random.uniform(20, 500), 2),
                    'unit': random.choice(units),
                    'description': f'High quality {name}',
                    'is_active': True
                })
                product_id += 1
        
        while len(self.products_data) < self.num_products:
            category_id = random.randint(1, len(self.categories_data))
            self.products_data.append({
                'product_id': len(self.products_data) + 1,
                'product_name': f'Product {len(self.products_data) + 1}',
                'category_id': category_id,
                'price': round(random.uniform(20, 500), 2),
                'unit': random.choice(units),
                'description': 'General product',
                'is_active': True
            })
        
        return self.products_data
    
    def generate_customers(self):
        """Generate customer data"""
        first_names = ['Raj', 'Priya', 'Amit', 'Sneha', 'Vikram', 'Anjali', 'Rahul', 'Neha', 
                      'Arjun', 'Pooja', 'Karan', 'Divya', 'Rohan', 'Kavita', 'Sanjay']
        last_names = ['Sharma', 'Patel', 'Kumar', 'Singh', 'Verma', 'Gupta', 'Reddy', 
                     'Nair', 'Joshi', 'Chopra', 'Malhotra', 'Agarwal', 'Mehta', 'Shah']
        cities = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai', 'Kolkata', 
                 'Pune', 'Ahmedabad', 'Jaipur', 'Lucknow']
        states = ['Maharashtra', 'Delhi', 'Karnataka', 'Telangana', 'Tamil Nadu', 
                 'West Bengal', 'Maharashtra', 'Gujarat', 'Rajasthan', 'Uttar Pradesh']
        
        for i in range(1, self.num_customers + 1):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            city_idx = random.randint(0, len(cities) - 1)
            
            self.customers_data.append({
                'customer_id': i,
                'first_name': first_name,
                'last_name': last_name,
                'email': f'{first_name.lower()}.{last_name.lower()}{i}@email.com',
                'phone': f'+91{random.randint(7000000000, 9999999999)}',
                'address': f'{random.randint(1, 999)} Main Street',
                'city': cities[city_idx],
                'state': states[city_idx],
                'zip_code': f'{random.randint(100000, 999999)}',
                'registration_date': (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d'),
                'is_active': True
            })
        
        return self.customers_data
    
    def generate_delivery_personnel(self):
        """Generate delivery personnel data"""
        first_names = ['Ravi', 'Suresh', 'Anil', 'Prakash', 'Vijay', 'Manoj', 'Deepak', 'Ramesh']
        last_names = ['Kumar', 'Singh', 'Yadav', 'Prasad', 'Das', 'Roy', 'Sharma']
        vehicles = ['Motorcycle', 'Bicycle', 'Scooter', 'Car', 'Van']
        
        for i in range(1, self.num_delivery_personnel + 1):
            self.delivery_personnel_data.append({
                'personnel_id': i,
                'first_name': random.choice(first_names),
                'last_name': random.choice(last_names),
                'phone': f'+91{random.randint(7000000000, 9999999999)}',
                'vehicle_type': random.choice(vehicles),
                'license_number': f'DL{random.randint(100000, 999999)}',
                'rating': round(random.uniform(3.5, 5.0), 2),
                'total_deliveries': random.randint(50, 1000),
                'status': random.choice(['available', 'busy', 'offline']),
                'hired_date': (datetime.now() - timedelta(days=random.randint(30, 1000))).strftime('%Y-%m-%d'),
                'is_active': True
            })
        
        return self.delivery_personnel_data
    
    def generate_orders_and_items(self):
        """Generate orders and order items"""
        payment_methods = ['Credit Card', 'Debit Card', 'UPI', 'Cash on Delivery', 'Net Banking']
        order_statuses = ['pending', 'confirmed', 'processing', 'out_for_delivery', 'delivered', 'cancelled']
        
        order_item_id = 1
        
        for i in range(1, self.num_orders + 1):
            customer_id = random.randint(1, self.num_customers)
            customer = self.customers_data[customer_id - 1]
            
            order_date = datetime.now() - timedelta(days=random.randint(0, 90))
            num_items = random.randint(1, 10)
            
            items = []
            total_amount = 0
            
            for _ in range(num_items):
                product_id = random.randint(1, len(self.products_data))
                product = self.products_data[product_id - 1]
                quantity = random.randint(1, 5)
                unit_price = product['price']
                subtotal = quantity * unit_price
                total_amount += subtotal
                
                items.append({
                    'order_item_id': order_item_id,
                    'order_id': i,
                    'product_id': product_id,
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'subtotal': round(subtotal, 2)
                })
                order_item_id += 1
            
            discount_amount = round(total_amount * random.uniform(0, 0.15), 2)
            tax_amount = round((total_amount - discount_amount) * 0.05, 2)
            final_amount = round(total_amount - discount_amount + tax_amount, 2)
            
            self.orders_data.append({
                'order_id': i,
                'customer_id': customer_id,
                'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
                'total_amount': round(total_amount, 2),
                'discount_amount': discount_amount,
                'tax_amount': tax_amount,
                'final_amount': final_amount,
                'payment_method': random.choice(payment_methods),
                'payment_status': random.choice(['pending', 'completed', 'failed']),
                'order_status': random.choice(order_statuses),
                'delivery_address': customer['address'],
                'delivery_city': customer['city'],
                'delivery_zip': customer['zip_code'],
                'special_instructions': random.choice(['', '', '', 'Leave at door', 'Call on arrival'])
            })
            
            self.order_items_data.extend(items)
        
        return self.orders_data, self.order_items_data
    
    def generate_deliveries(self):
        """Generate delivery data for orders"""
        delivery_statuses = ['pending', 'assigned', 'picked_up', 'in_transit', 'delivered', 'failed']
        
        for order in self.orders_data[:int(len(self.orders_data) * 0.8)]:  # 80% of orders have deliveries
            order_date = datetime.strptime(order['order_date'], '%Y-%m-%d %H:%M:%S')
            pickup_time = order_date + timedelta(minutes=random.randint(15, 60))
            estimated_delivery = pickup_time + timedelta(minutes=random.randint(20, 90))
            actual_delivery = estimated_delivery + timedelta(minutes=random.randint(-15, 30))
            
            self.deliveries_data.append({
                'delivery_id': len(self.deliveries_data) + 1,
                'order_id': order['order_id'],
                'personnel_id': random.randint(1, self.num_delivery_personnel),
                'pickup_time': pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
                'estimated_delivery_time': estimated_delivery.strftime('%Y-%m-%d %H:%M:%S'),
                'actual_delivery_time': actual_delivery.strftime('%Y-%m-%d %H:%M:%S') if random.random() > 0.1 else '',
                'delivery_status': random.choice(delivery_statuses),
                'distance_km': round(random.uniform(2, 25), 2),
                'delivery_rating': round(random.uniform(3.0, 5.0), 2) if random.random() > 0.3 else '',
                'customer_feedback': random.choice(['', '', 'Great service!', 'Fast delivery', 'Good experience'])
            })
        
        return self.deliveries_data
    
    def generate_inventory(self):
        """Generate inventory data"""
        for product in self.products_data:
            self.inventory_data.append({
                'inventory_id': product['product_id'],
                'product_id': product['product_id'],
                'quantity_available': random.randint(0, 500),
                'quantity_reserved': random.randint(0, 50),
                'reorder_level': random.randint(20, 100),
                'last_restock_date': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
                'warehouse_location': f'Warehouse-{random.choice(["A", "B", "C", "D"])}'
            })
        
        return self.inventory_data
    
    def save_to_csv(self):
        """Save all generated data to CSV files"""
        datasets = {
            'categories.csv': self.categories_data,
            'products.csv': self.products_data,
            'customers.csv': self.customers_data,
            'delivery_personnel.csv': self.delivery_personnel_data,
            'orders.csv': self.orders_data,
            'order_items.csv': self.order_items_data,
            'deliveries.csv': self.deliveries_data,
            'inventory.csv': self.inventory_data
        }
        
        for filename, data in datasets.items():
            filepath = os.path.join(self.output_dir, filename)
            if data:
                df = pd.DataFrame(data)
                df.to_csv(filepath, index=False)
                print(f"✓ Generated {filename} with {len(data)} records")
    
    def generate_all(self):
        """Generate all data"""
        print("Starting data generation...")
        print("=" * 60)
        
        self.generate_categories()
        self.generate_products()
        self.generate_customers()
        self.generate_delivery_personnel()
        self.generate_orders_and_items()
        self.generate_deliveries()
        self.generate_inventory()
        
        self.save_to_csv()
        
        print("=" * 60)
        print("Data generation complete!")
        print(f"Output directory: {self.output_dir}")

if __name__ == "__main__":
    generator = GroceryDataGenerator()
    generator.generate_all()
