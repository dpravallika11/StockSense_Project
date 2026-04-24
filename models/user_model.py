from werkzeug.security import generate_password_hash, check_password_hash

class UserModel:
    def __init__(self, db):
        self.collection = db.users

    def create_user(self, store_name, username, password, role="Cashier"):
        if self.collection.find_one({"username": username}):
            return None # User already exists
            
        hashed_password = generate_password_hash(password)
        user_doc = {
            "store_name": store_name,
            "username": username,
            "password": hashed_password,
            "role": role
        }
        result = self.collection.insert_one(user_doc)
        user_doc["_id"] = result.inserted_id
        return user_doc

    def verify_user(self, username, password):
        user = self.collection.find_one({"username": username})
        if user and check_password_hash(user["password"], password):
            return user
        return None
