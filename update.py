from pymongo import MongoClient
import os
import sys
import re
import argparse
import unicodedata
import time
import traceback
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()


def normalize_text(value: str | None) -> str | None:
    if not value:
        return None
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def parse_location(location: str | None):
    cleaned = normalize_text(location)
    if not cleaned:
        return None, None

    parts = [p.strip() for p in re.split(r"[,/;\n]+", cleaned) if p and p.strip()]
    if not parts:
        return None, None

    city = parts[0]
    department = parts[1] if len(parts) > 1 else None
    return city, department


def init_db():
    """Initialize MongoDB connection using either MONGO_URI or user/password vars."""
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB", "scraping")
    collection_name = os.getenv("MONGO_COLLECTION_FREEWORK") or os.getenv("MONGO_COLLECTION") or "freework"

    if uri:
        print(f"Using MONGO_URI for db={db_name}, collection={collection_name}", file=sys.stderr)
        try:
            client = MongoClient(uri)
            client.admin.command("ping")
            print("MongoDB connection successful", file=sys.stderr)
            return client, client[db_name][collection_name]
        except Exception as e:
            print(f"MongoDB connection failed via MONGO_URI: {e}", file=sys.stderr)
            sys.exit(1)

    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    host = os.getenv("MONGO_HOST")

    print(
        f"Fallback env: user={user}, host={host}, db={db_name}, collection={collection_name}, password_set={bool(password)}",
        file=sys.stderr,
    )
    if not all([user, password, host, db_name, collection_name]):
        print(
            "Missing env vars. Provide MONGO_URI or MONGO_USER/MONGO_PASSWORD/MONGO_HOST/MONGO_DB/MONGO_COLLECTION(_FREEWORK).",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        encoded_password = quote(password or "")
        uri = f"mongodb+srv://{user}:{encoded_password}@{host}/{db_name}?retryWrites=true&w=majority"
        client = MongoClient(uri)
        client.admin.command("ping")
        print("MongoDB connection successful", file=sys.stderr)
        return client, client[db_name][collection_name]
    except Exception as e:
        print(f"MongoDB connection failed via user/password: {e}", file=sys.stderr)
        sys.exit(1)


def migrate_locations(collection, dry_run: bool = True):
    # Fast path: only process documents where BOTH fields are missing/empty.
    query = {
        "$and": [
            {
                "$or": [
                    {"city": {"$exists": False}},
                    {"city": None},
                    {"city": ""},
                ]
            },
            {
                "$or": [
                    {"department": {"$exists": False}},
                    {"department": None},
                    {"department": ""},
                ]
            },
        ]
    }

    cursor = collection.find(query, {"_id": 1, "id": 1, "location": 1, "city": 1, "department": 1})

    scanned = 0
    updated = 0
    skipped = 0

    for doc in cursor:
        scanned += 1
        city_from_location, department_from_location = parse_location(doc.get("location"))

        existing_city = normalize_text(doc.get("city"))
        existing_department = normalize_text(doc.get("department"))

        new_city = existing_city or city_from_location
        new_department = existing_department or department_from_location

        if new_city == doc.get("city") and new_department == doc.get("department"):
            skipped += 1
            continue

        update_fields = {"city": new_city, "department": new_department}

        if dry_run:
            print(
                f"[DRY-RUN] id={doc.get('id')} location='{doc.get('location')}' => city='{new_city}', department='{new_department}'",
                file=sys.stderr,
            )
        else:
            collection.update_one({"_id": doc["_id"]}, {"$set": update_fields})
            print(
                f"Updated id={doc.get('id')} => city='{new_city}', department='{new_department}'",
                file=sys.stderr,
            )
        updated += 1

    print(
        f"Migration finished. scanned={scanned}, updated={updated}, skipped={skipped}, dry_run={dry_run}",
        file=sys.stderr,
    )


def main():
    parser = argparse.ArgumentParser(description="Populate city and department fields from freework location")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default is dry-run)")
    parser.add_argument(
        "--every-hour",
        action="store_true",
        help="Run migration every hour continuously",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=3600,
        help="Interval in seconds for repeated mode (default: 3600)",
    )
    args = parser.parse_args()

    if args.every_hour:
        print(
            f"Starting repeated mode. interval={args.interval_seconds}s, apply={args.apply}",
            file=sys.stderr,
        )
        while True:
            client = None
            try:
                client, collection = init_db()
                migrate_locations(collection, dry_run=not args.apply)
            except KeyboardInterrupt:
                print("Stopped by user", file=sys.stderr)
                break
            except Exception as e:
                print(f"Run failed: {e}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
            finally:
                if client is not None:
                    client.close()
            try:
                time.sleep(args.interval_seconds)
            except KeyboardInterrupt:
                print("Stopped by user", file=sys.stderr)
                break
    else:
        client, collection = init_db()
        try:
            migrate_locations(collection, dry_run=not args.apply)
        finally:
            client.close()


if __name__ == "__main__":
    main()
