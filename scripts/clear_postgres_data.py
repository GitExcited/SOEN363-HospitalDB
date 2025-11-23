"""
Clear PostgreSQL data to start fresh
"""

import psycopg2
import sys

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "admin",
    "password": "admin",
    "database": "hospital_db"
}

def main():
    try:
        print("[POSTGRES] Connecting to PostgreSQL...")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        print("✓ Connected")

        print("\n[CLEAR] Clearing all data...")

        # Disable foreign keys
        cursor.execute("SET session_replication_role = 'replica'")

        # Delete in reverse order of dependencies
        cursor.execute("DELETE FROM diagnoses_icd")
        print("✓ Cleared diagnoses_icd")

        cursor.execute("DELETE FROM icustays")
        print("✓ Cleared icustays")

        cursor.execute("DELETE FROM noteevents")
        print("✓ Cleared noteevents")

        cursor.execute("DELETE FROM admissions")
        print("✓ Cleared admissions")

        cursor.execute("DELETE FROM patients")
        print("✓ Cleared patients")

        # Re-enable foreign keys
        cursor.execute("SET session_replication_role = 'origin'")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n✓ Database cleared successfully")

    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
