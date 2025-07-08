from pymongo import MongoClient
import os
from urllib.parse import quote

def init_db():
    """Initialize MongoDB connection using environment variables."""
    user = os.getenv("MONGO_USER")
    password = quote(os.getenv("MONGO_PASSWORD"))  # URL-encode password
    host = os.getenv("MONGO_HOST")
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")

    if not all([user, password, host, db_name, collection_name]):
        raise ValueError(f"Missing environment variables: user={user}, host={host}, db={db_name}, collection={collection_name}")

    uri = f"mongodb+srv://{user}:{password}@{host}/{db_name}?retryWrites=true&w=majority"
    try:
        client = MongoClient(uri)
        # Test connection
        client.admin.command('ping')
        db = client[db_name]
        collection = db[collection_name]
        return client, collection
    except Exception as e:
        raise Exception(f"MongoDB connection failed: {e}")

def insert_job(collection, job):
    """Insert or update a job document in the MongoDB collection."""
    try:
        collection.replace_one(
            {'id': job['id']},
            job,
            upsert=True
        )
    except Exception as e:
        raise Exception(f"Failed to insert job: {e}")