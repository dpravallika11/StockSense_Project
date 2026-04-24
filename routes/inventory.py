from flask import Blueprint, jsonify, request
from bson.objectid import ObjectId
from models.product_model import ProductModel
from models.inventory_model import InventoryModel
from database import db
from utils.decorators import login_required, admin_required

inventory_bp = Blueprint('inventory', __name__)
product_model = ProductModel(db)
inventory_model = InventoryModel(db)

@inventory_bp.route('/api/products', methods=['GET'])
@login_required
def get_products():
    products = product_model.get_all_products()
    for p in products: p['_id'] = str(p['_id'])
    return jsonify(products)
    
@inventory_bp.route('/api/products', methods=['POST'])
@admin_required
def add_product():
    data = request.json
    result = product_model.add_product(
        data['name'], data['category'], data['price'], data['barcode'], data['min_stock']
    )
    if result: return jsonify({"success": True}), 201
    return jsonify({"error": "Barcode exists"}), 400

@inventory_bp.route('/api/batches', methods=['POST'])
@admin_required
def add_batch():
    data = request.json
    from datetime import datetime
    try:
        expiry = datetime.strptime(data['expiry_date'], "%Y-%m-%d")
    except:
        return jsonify({"error": "Invalid date format, use YYYY-MM-DD"}), 400
        
    inventory_model.add_batch(
        ObjectId(data['product_id']), data['batch_id'], data['quantity'], expiry
    )
    return jsonify({"success": True}), 201

@inventory_bp.route('/api/products/<product_id>', methods=['DELETE'])
@admin_required
def delete_product(product_id):
    try:
        product_model.delete_product(product_id)
        return jsonify({"success": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400
