from datetime import datetime, timedelta

def check_expiry_and_alerts(db):
    print(f"[{datetime.now()}] Running background task: Expiry and Low Stock Checks")
    
    try:
        # 1. Check for near-expiry products (within 7 days)
        upcoming_expiry_date = datetime.now() + timedelta(days=7)
        expiring_batches = list(db.inventory.find({
            "status": "active",
            "expiry_date": {"$lte": upcoming_expiry_date}
        }))
        
        for batch in expiring_batches:
            try:
                batch_id = batch.get("batch_id", str(batch["_id"]))
                
                # Check if alert already exists to avoid duplicates
                existing_alert = db.alerts.find_one({
                    "type": "expiry",
                    "batch_id": batch_id,
                    "status": "unread"
                })
                if not existing_alert:
                    expiry_date = batch["expiry_date"]
                    days_left = (expiry_date - datetime.now()).days
                    # Suggest discount dynamically
                    discount = "10%" if days_left > 3 else "25%"
                    
                    db.alerts.insert_one({
                        "type": "expiry",
                        "batch_id": batch_id,
                        "product_id": batch.get("product_id"),
                        "message": f"Batch {batch_id} expires in {max(0, days_left)} days. Suggestion: Apply {discount} discount.",
                        "created_at": datetime.now(),
                        "status": "unread"
                    })
                    print(f"  [Alert Created] Expiry alert for batch {batch_id}")
            except Exception as e:
                print(f"  [Error] Failed to process expiry batch {batch.get('_id')}: {e}")
                
    except Exception as e:
        print(f"  [Error] Expiry check failed: {e}")
            
    try:
        # 2. Check for low stock
        products = list(db.products.find())
        for product in products:
            try:
                # aggregate total active stock for this product
                pipeline = [
                    {"$match": {"product_id": product["_id"], "status": "active"}},
                    {"$group": {"_id": None, "total": {"$sum": "$quantity"}}}
                ]
                result = list(db.inventory.aggregate(pipeline))
                total_stock = result[0]["total"] if result else 0
                min_stock = product.get("min_stock", 0)
                
                if total_stock < min_stock:
                    existing_stock_alert = db.alerts.find_one({
                        "type": "low_stock",
                        "product_id": product["_id"],
                        "status": "unread"
                    })
                    if not existing_stock_alert:
                        db.alerts.insert_one({
                            "type": "low_stock",
                            "product_id": product["_id"],
                            "message": f"Low stock for {product['name']}. Current stock: {total_stock}. Minimum: {min_stock}.",
                            "created_at": datetime.now(),
                            "status": "unread"
                        })
                        print(f"  [Alert Created] Low stock alert for {product['name']}")
            except Exception as e:
                print(f"  [Error] Failed to process low-stock for {product.get('name')}: {e}")
    except Exception as e:
        print(f"  [Error] Low stock check failed: {e}")

def init_scheduler(app, db):
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    import atexit
    
    scheduler = BackgroundScheduler()
    # Run every 5 minutes AND immediately at startup (next_run_time=datetime.now())
    scheduler.add_job(
        func=lambda: check_expiry_and_alerts(db),
        trigger=IntervalTrigger(minutes=5),
        next_run_time=datetime.now()  # Run immediately on startup
    )
    scheduler.start()
    print("[Scheduler] Background alert scheduler started. Running initial check now...")
    
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())
