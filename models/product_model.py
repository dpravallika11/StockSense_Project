from bson.objectid import ObjectId

class ProductModel:
    def __init__(self, db):
        self.collection = db.products

    def get_all_products(self):
        return list(self.collection.find())

    def get_product(self, product_id):
        return self.collection.find_one({"_id": ObjectId(product_id)})

    def add_product(self, name, category, price, barcode, min_stock):
        if self.collection.find_one({"barcode": barcode}):
            return None # Barcode already exists
            
        doc = {
            "name": name,
            "category": category,
            "price": float(price),
            "barcode": barcode,
            "min_stock": int(min_stock)
        }
        return self.collection.insert_one(doc).inserted_id

    def update_product(self, product_id, data):
        self.collection.update_one({"_id": ObjectId(product_id)}, {"$set": data})

    def delete_product(self, product_id):
        self.collection.delete_one({"_id": ObjectId(product_id)})
