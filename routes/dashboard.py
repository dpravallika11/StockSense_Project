from flask import Blueprint, render_template, session
from database import db
from utils.decorators import admin_required
from datetime import datetime, timedelta

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
@admin_required
def index():
    # 1. Overview Metrics
    total_sales = db.sales.count_documents({})
    
    # Calculate revenue for last 7 days
    last_week = datetime.now() - timedelta(days=7)
    revenue_pipeline = [
        {"$match": {"sale_date": {"$gte": last_week}}},
        {"$group": {"_id": None, "total": {"$sum": "$total_price"}}}
    ]
    revenue_result = list(db.sales.aggregate(revenue_pipeline))
    weekly_revenue = revenue_result[0]['total'] if revenue_result else 0
    
    # Active inventory count
    inventory_count = db.inventory.count_documents({"status": "active"})
    
    # Total products count
    total_products = db.products.count_documents({})
    
    # Unread alerts count
    alerts_count = db.alerts.count_documents({"status": "unread"})
    
    # Pending purchase orders count
    po_count = db.purchase_orders.count_documents({"status": "pending"})

    # 2. Get Alerts
    alerts = list(db.alerts.find({"status": "unread"}).sort("created_at", -1).limit(10))
    for alert in alerts:
        try:
            product_id = alert.get("product_id")
            product = db.products.find_one({"_id": product_id}) if product_id else None
            alert["product_name"] = product["name"] if product else "Unknown"
        except Exception:
            alert["product_name"] = "Unknown"
        # Ensure template-friendly id strings (avoid ObjectId(...) JS/HTML issues)
        try:
            alert["_id"] = str(alert["_id"])
        except Exception:
            pass

    # 3. Get Forecasts & Purchase Orders
    forecasts = list(db.forecasts.find().sort("last_updated", -1).limit(5))
    for f in forecasts:
        product = db.products.find_one({"_id": f["product_id"]})
        f["product_name"] = product["name"] if product else "Unknown"
        
    pos = list(db.purchase_orders.find({"status": "pending"}).sort("created_at", -1))
    for po in pos:
        product = db.products.find_one({"_id": po["product_id"]})
        po["product_name"] = product["name"] if product else "Unknown"

    # 4. Chart Data (Sales Trend for last 7 days)
    chart_labels = [(datetime.now() - timedelta(days=i)).strftime('%b %d') for i in range(6, -1, -1)]
    chart_data = [0] * 7
    
    daily_sales = list(db.sales.aggregate([
        {"$match": {"sale_date": {"$gte": last_week}}},
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%b %d", "date": "$sale_date"}},
                "total": {"$sum": "$total_price"}
            }
        }
    ]))
    
    for item in daily_sales:
        if item["_id"] in chart_labels:
            idx = chart_labels.index(item["_id"])
            chart_data[idx] = item["total"]
            
    # 5. Products list for Add Batch Order form
    products_list = list(db.products.find())
    # stringify product ids for safe HTML values
    for p in products_list:
        try:
            p["_id"] = str(p["_id"])
        except Exception:
            pass
    
    # 6. Analysis and Discount Suggestions (Expiry within 30 days)
    upcoming_expiry = datetime.now() + timedelta(days=30)
    expiring_batches = list(db.inventory.find({
        "status": "active",
        "expiry_date": {"$lte": upcoming_expiry}
    }))
    
    discounts = []
    for batch in expiring_batches:
        prod = db.products.find_one({"_id": batch["product_id"]})
        if prod:
            days_to_expiry = (batch["expiry_date"] - datetime.now()).days
            discount_pct = 50 if days_to_expiry < 7 else 20
            discounts.append({
                "product_name": prod["name"],
                "batch_id": batch.get("batch_id", "N/A"),
                "days_left": max(0, days_to_expiry),
                "quantity": batch["quantity"],
                "suggested_discount": f"{discount_pct}% OFF"
            })

    return render_template('dashboard.html', 
                           weekly_revenue=weekly_revenue, 
                           total_sales=total_sales,
                           inventory_count=inventory_count,
                           total_products=total_products,
                           alerts_count=alerts_count,
                           po_count=po_count,
                           alerts=alerts,
                           forecasts=forecasts,
                           purchase_orders=pos,
                           chart_labels=chart_labels,
                           chart_data=chart_data,
                           products=products_list,
                           discounts=discounts)

@dashboard_bp.route('/dismiss_alert/<alert_id>', methods=['POST'])
@admin_required
def dismiss_alert(alert_id):
    from bson.objectid import ObjectId
    try:
        db.alerts.update_one({"_id": ObjectId(alert_id)}, {"$set": {"status": "dismissed"}})
        return {"success": True}, 200
    except Exception as e:
        return {"error": str(e)}, 400
