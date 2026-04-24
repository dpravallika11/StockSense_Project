from app import db
from werkzeug.security import generate_password_hash
from datetime import datetime, timedelta
import random

def seed_database():
    print("Clearing database...")
    db.users.drop()
    db.products.drop()
    db.inventory.drop()
    db.sales.drop()
    db.forecasts.drop()
    db.alerts.drop()
    db.purchase_orders.drop()

    print("Seeding Users...")
    users = [
        {
            "store_name": "Test Store",
            "username": "admin",
            "password": generate_password_hash("admin123"),
            "role": "Admin"
        },
        {
            "store_name": "Test Store",
            "username": "cashier",
            "password": generate_password_hash("cashier123"),
            "role": "Cashier"
        }
    ]
    db.users.insert_many(users)

    print("Seeding Products and Inventory...")
    products = [
        {"name": "Milk 1L", "category": "Dairy", "price": 40.50, "barcode": "111111", "min_stock": 20},
        {"name": "Bread", "category": "Bakery", "price": 30.00, "barcode": "222222", "min_stock": 15},
        {"name": "Eggs 12pk", "category": "Dairy", "price": 60.00, "barcode": "333333", "min_stock": 10},
        {"name": "Apples 1kg", "category": "Produce", "price": 120.00, "barcode": "444444", "min_stock": 25},
        {"name": "Pasta 500g", "category": "Pantry", "price": 45.00, "barcode": "555555", "min_stock": 50}
    ]
    
    product_ids = []
    for p in products:
        result = db.products.insert_one(p)
        product_ids.append(result.inserted_id)

    inventory = []
    for pid in product_ids:
        # Create 2 batches per product
        for i in range(2):
            # Random expiry between 2 and 30 days
            expiry = datetime.now() + timedelta(days=random.randint(2, 30))
            inventory.append({
                "product_id": pid,
                "batch_id": f"B{random.randint(1000, 9999)}",
                "quantity": random.randint(15, 40),
                "expiry_date": expiry,
                "status": "active"
            })
    db.inventory.insert_many(inventory)

    print("Seeding Historical Sales...")
    # Generate random sales for the past 30 days
    sales = []
    for i in range(30):
        sale_date = datetime.now() - timedelta(days=i)
        
        # Random number of sales per day (5 to 20)
        for _ in range(random.randint(5, 20)):
            product_id = random.choice(product_ids)
            qty = random.randint(1, 5)
            product = db.products.find_one({"_id": product_id})
            
            sales.append({
                "product_id": product_id,
                "quantity": qty,
                "total_price": qty * product["price"],
                "sale_date": sale_date,
                "cashier": "cashier"
            })
            
    db.sales.insert_many(sales)

    print("Seeding complete!")

if __name__ == "__main__":
    seed_database()
