from flask import Blueprint, jsonify, request, session, render_template
from bson.objectid import ObjectId
from models.product_model import ProductModel
from models.inventory_model import InventoryModel
from database import db
from utils.decorators import login_required
from datetime import datetime

pos_bp = Blueprint('pos', __name__)
product_model = ProductModel(db)
inventory_model = InventoryModel(db)

@pos_bp.route('/')
@login_required
def index():
    return render_template('pos.html')

@pos_bp.route('/api/search', methods=['GET'])
@login_required
def search_product():
    query = request.args.get('q', '')
    if not query:
        results = db.products.find()
    else:
        # Search by barcode or name (case-insensitive)
        results = db.products.find({
            "$or": [
                {"barcode": query},
                {"name": {"$regex": query, "$options": "i"}}
            ]
        })
    
    products = []
    for p in results:
        p['_id'] = str(p['_id'])
        # Get active stock for this product
        pipeline = [
            {"$match": {"product_id": ObjectId(p['_id']), "status": "active"}},
            {"$group": {"_id": None, "total": {"$sum": "$quantity"}}}
        ]
        stock_result = list(db.inventory.aggregate(pipeline))
        p['current_stock'] = stock_result[0]["total"] if stock_result else 0
        products.append(p)
        
    return jsonify(products)

@pos_bp.route('/api/checkout', methods=['POST'])
@login_required
def checkout():
    data = request.json
    cart = data.get('cart', [])
    
    if not cart:
        return jsonify({"error": "Cart is empty"}), 400
        
    # Validation step: Check if enough stock exists for all items before deducting
    for item in cart:
        product_id = ObjectId(item['product_id'])
        qty_needed = int(item['quantity'])
        
        # Get active stock
        pipeline = [
            {"$match": {"product_id": product_id, "status": "active"}},
            {"$group": {"_id": None, "total": {"$sum": "$quantity"}}}
        ]
        stock_result = list(db.inventory.aggregate(pipeline))
        total_stock = stock_result[0]["total"] if stock_result else 0
        
        if total_stock < qty_needed:
            return jsonify({"error": f"Insufficient stock for {item['name']}. Available: {total_stock}"}), 400

    # Deduction step: FEFO Logic application
    for item in cart:
        product_id = ObjectId(item['product_id'])
        qty_needed = int(item['quantity'])
        
        # Deduct using FEFO
        inventory_model.deduct_stock_fefo(product_id, qty_needed)
        
        # Record Sale
        db.sales.insert_one({
            "product_id": product_id,
            "quantity": qty_needed,
            "total_price": float(item['price']) * qty_needed,
            "sale_date": datetime.now(),
            "cashier": session.get('username')
        })
        
    return jsonify({"success": True, "message": "Checkout completed successfully."}), 200
