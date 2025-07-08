from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

def init_db():
    """Initialize MongoDB connection using environment variables."""
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    host = os.getenv("MONGO_HOST")
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")

    uri = f"mongodb+srv://{user}:{password}@{host}/{db_name}?retryWrites=true&w=majority"
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return client, collection

def insert_job(collection, job):
    """Insert or update a job document in the MongoDB collection."""
    collection.replace_one(
        {'id': job['id']},
        job,
        upsert=True
    )
