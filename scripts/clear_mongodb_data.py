"""
Clear MongoDB data to start fresh
"""

import pymongo
import sys

MONGODB_CONFIG = {
    "host": "localhost",
    "port": 27018,
    "user": "admin",
    "password": "admin",
    "database": "hospital_db_nosql"
}

def main():
    try:
        print("[MONGODB] Connecting to MongoDB...")
        client = pymongo.MongoClient(
            f"mongodb://{MONGODB_CONFIG['user']}:{MONGODB_CONFIG['password']}@{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}/",
            serverSelectionTimeoutMS=5000
        )
        db = client[MONGODB_CONFIG['database']]
        print("✓ Connected")

        print("\n[CLEAR] Clearing all collections...")

        # Drop collections
        if 'patients' in db.list_collection_names():
            db['patients'].drop()
            print("✓ Cleared patients collection")

        if 'noteevents' in db.list_collection_names():
            db['noteevents'].drop()
            print("✓ Cleared noteevents collection")

        client.close()

        print("\n✓ MongoDB cleared successfully")

    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
