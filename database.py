import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/stocksense")
client = MongoClient(mongo_uri)
db = client.get_database()
