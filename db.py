from pymongo import MongoClient
import os
import sys
from urllib.parse import quote
from dotenv import load_dotenv
load_dotenv()

def init_db():
    """Initialize MongoDB connection using environment variables."""
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    host = os.getenv("MONGO_HOST")
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")

    print(f"Environment variables: user={user}, host={host}, db={db_name}, collection={collection_name}, password_set={bool(password)}", file=sys.stderr)
    if not all([user, password, host, db_name, collection_name]):
        print(f"Missing environment variables: user={user}, password_set={bool(password)}, host={host}, db={db_name}, collection={collection_name}", file=sys.stderr)
        sys.exit(1)

    try:
        encoded_password = quote(password)
    except Exception as e:
        print(f"Password encoding failed: {e}", file=sys.stderr)
        sys.exit(1)

    uri = f"mongodb+srv://{user}:{encoded_password}@{host}/{db_name}?retryWrites=true&w=majority"
    print(f"Attempting MongoDB connection with URI: mongodb+srv://{user}:[REDACTED]@{host}/{db_name}", file=sys.stderr)
    try:
        client = MongoClient(uri)
        client.admin.command('ping')
        print("MongoDB connection successful", file=sys.stderr)
        db = client[db_name]
        collection = db[collection_name]
        return client, collection
    except Exception as e:
        print(f"MongoDB connection failed: {e}", file=sys.stderr)
        sys.exit(1)

def insert_job(collection, job):
    """Insert or update a job document in the MongoDB collection."""
    try:
        collection.replace_one(
            {'id': job['id']},
            job,
            upsert=True
        )
        print(f"Inserted job with id: {job['id']}", file=sys.stderr)
    except Exception as e:
        print(f"Failed to insert job: {e}", file=sys.stderr)
        sys.exit(1)