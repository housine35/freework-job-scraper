from pymongo import MongoClient
import os
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

def init_db():
    """Initialize MongoDB connection using environment variables."""
    user = os.getenv("MONGO_USER")
    password = quote(os.getenv("MONGO_PASSWORD"))  # URL-encode password
    host = os.getenv("MONGO_HOST")
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")

    if not all([user, password, host, db_name, collection_name]):
        raise ValueError("One or more environment variables are missing")

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