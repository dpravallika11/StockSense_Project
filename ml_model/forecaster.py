import pandas as pd
import numpy as np
import os
import sys

# Add parent directory to path to import db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import db

from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta

def generate_forecasts():
    print(f"[{datetime.now()}] Generating demand forecasts...")
    
    # 1. Gather historical sales data
    sales = list(db.sales.find())
    if not sales:
        print("No sales data available for forecasting.")
        return
        
    df = pd.DataFrame(sales)
    df['sale_date'] = pd.to_datetime(df['sale_date']).dt.date
    
    # 2. Group by product_id
    products = db.products.find()
    for product in products:
        prod_id = product["_id"]
        prod_sales = df[df['product_id'] == prod_id]
        
        if prod_sales.empty or len(prod_sales['sale_date'].unique()) < 3:
            # Need at least 3 days of data for a meaningful trend line
            continue
            
        # Aggregate logic by day
        daily_sales = prod_sales.groupby('sale_date')['quantity'].sum().reset_index()
        
        # Convert dates to ordinal for linear regression
        daily_sales['date_ordinal'] = pd.to_datetime(daily_sales['sale_date']).map(datetime.toordinal)
        
        X = daily_sales[['date_ordinal']]
        y = daily_sales['quantity']
        
        # Train model
        model = LinearRegression()
        model.fit(X, y)
        
        # Predict next 7 days
        future_dates = [(datetime.now() + timedelta(days=i)).date() for i in range(1, 8)]
        X_future = pd.DataFrame({'date_ordinal': [d.toordinal() for d in future_dates]})
        
        predictions = model.predict(X_future)
        predictions = [max(0, int(round(p))) for p in predictions] # cannot have negative demand
        
        total_predicted_demand = sum(predictions)
        
        # Save forecast to db
        db.forecasts.update_one(
            {"product_id": prod_id},
            {"$set": {
                "last_updated": datetime.now(),
                "next_7_days_predicted": total_predicted_demand,
                "daily_predictions": predictions
            }},
            upsert=True
        )
        
        # ---------------------------------------------
        # Smart Purchase Order System
        # ---------------------------------------------
        # Check current stock
        pipeline = [
            {"$match": {"product_id": prod_id, "status": "active"}},
            {"$group": {"_id": None, "total": {"$sum": "$quantity"}}}
        ]
        res = list(db.inventory.aggregate(pipeline))
        current_stock = res[0]["total"] if res else 0
        
        # If predicted demand > current stock + buffer, suggest order
        min_stock = product.get("min_stock", 10)
        if current_stock < total_predicted_demand:
            shortage = total_predicted_demand - current_stock
            priority = "High" if current_stock == 0 else "Medium"
            
            # Check if pending order already exists
            existing_po = db.purchase_orders.find_one({
                "product_id": prod_id,
                "status": "pending"
            })
            
            if not existing_po:
                db.purchase_orders.insert_one({
                    "product_id": prod_id,
                    "suggested_quantity": shortage + min_stock,
                    "priority": priority,
                    "status": "pending",
                    "created_at": datetime.now()
                })
                
    print("Forecasting complete.")

if __name__ == "__main__":
    generate_forecasts()
