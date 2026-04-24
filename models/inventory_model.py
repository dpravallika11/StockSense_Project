class InventoryModel:
    def __init__(self, db):
        self.collection = db.inventory

    def add_batch(self, product_id, batch_id, quantity, expiry_date):
        doc = {
            "product_id": product_id,
            "batch_id": batch_id,
            "quantity": int(quantity),
            "expiry_date": expiry_date,
            "status": "active"
        }
        return self.collection.insert_one(doc)

    def get_batches_by_product(self, product_id):
        return list(self.collection.find({"product_id": product_id, "status": "active"}).sort("expiry_date", 1))

    # FEFO logic to deduct stock
    def deduct_stock_fefo(self, product_id, required_qty):
        batches = self.get_batches_by_product(product_id)
        deducted = 0
        updates = []
        
        for batch in batches:
            if deducted >= required_qty:
                break
                
            available = batch["quantity"]
            needed = required_qty - deducted
            
            if available > needed:
                # Deduct part of the batch
                new_qty = available - needed
                updates.append((batch["_id"], new_qty, "active"))
                deducted += needed
            else:
                # Deduct whole batch
                updates.append((batch["_id"], 0, "depleted"))
                deducted += available
                
        if deducted < required_qty:
            return False # Not enough stock

        # Apply updates
        for batch_id, new_qty, status in updates:
            self.collection.update_one(
                {"_id": batch_id},
                {"$set": {"quantity": new_qty, "status": status}}
            )
            
        return True
