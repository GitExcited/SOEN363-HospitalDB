"""
SOEN363 Phase 2 - Load SQL Files to PostgreSQL
Execute generated SQL INSERT statements in PostgreSQL
"""

import psycopg2
import os
import sys
from datetime import datetime

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "admin",
    "password": "admin",
    "database": "hospital_db"
}

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def connect_to_postgres():
    """Connect to PostgreSQL database"""
    try:
        print("[POSTGRES] Connecting to PostgreSQL...")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("✓ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

def load_sql_file(conn, sql_file, label):
    """Load and execute SQL file"""
    try:
        print(f"\n[LOAD] Reading {label} SQL file: {sql_file}")

        if not os.path.exists(sql_file):
            print(f"✗ File not found: {sql_file}")
            return False

        file_size_mb = os.path.getsize(sql_file) / (1024 * 1024)
        print(f"  File size: {file_size_mb:.1f} MB")

        print(f"[EXECUTE] Executing {label} SQL statements...")
        cursor = conn.cursor()

        start_time = datetime.now()

        # Read and execute SQL file
        with open(sql_file, 'r') as f:
            sql_content = f.read()

        # Disable foreign key constraints for loading
        cursor.execute("SET session_replication_role = 'replica'")

        # Execute all statements
        cursor.execute(sql_content)

        # Re-enable foreign key constraints
        cursor.execute("SET session_replication_role = 'origin'")

        conn.commit()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"✓ Successfully executed {label} SQL file")
        print(f"  Duration: {duration:.2f} seconds")
        cursor.close()
        return True

    except Exception as e:
        print(f"✗ Error executing {label} SQL file: {str(e)[:200]}")
        conn.rollback()
        return False

def verify_data(conn):
    """Verify data was loaded correctly"""
    try:
        print("\n[VERIFY] Verifying data in PostgreSQL...")
        cursor = conn.cursor()

        # Count patients
        cursor.execute("SELECT COUNT(*) FROM patients")
        patient_count = cursor.fetchone()[0]
        print(f"✓ Patients: {patient_count:,}")

        # Count admissions
        cursor.execute("SELECT COUNT(*) FROM admissions")
        admission_count = cursor.fetchone()[0]
        print(f"✓ Admissions: {admission_count:,}")

        # Count ICU stays
        cursor.execute("SELECT COUNT(*) FROM icustays")
        icustay_count = cursor.fetchone()[0]
        print(f"✓ ICU stays: {icustay_count:,}")

        # Count diagnoses
        cursor.execute("SELECT COUNT(*) FROM diagnoses_icd")
        diagnoses_count = cursor.fetchone()[0]
        print(f"✓ Diagnoses: {diagnoses_count:,}")

        # Count noteevents
        cursor.execute("SELECT COUNT(*) FROM noteevents")
        noteevent_count = cursor.fetchone()[0]
        print(f"✓ Note events: {noteevent_count:,}")

        cursor.close()
        return True

    except Exception as e:
        print(f"✗ Verification failed: {e}")
        return False

def main():
    print("\n" + "=" * 70)
    print("SOEN363 PHASE 2 - LOAD SQL TO POSTGRESQL")
    print("Execute generated SQL INSERT statements")
    print("=" * 70)

    # Connect
    conn = connect_to_postgres()

    # Load patients SQL
    patients_sql = os.path.join(PROJECT_ROOT, "sql", "insert_scaled_patients.sql")
    if not load_sql_file(conn, patients_sql, "Patients"):
        print("✗ Failed to load patient data")
        conn.close()
        sys.exit(1)

    # Load noteevents SQL
    noteevents_sql = os.path.join(PROJECT_ROOT, "sql", "insert_scaled_noteevents.sql")
    if not load_sql_file(conn, noteevents_sql, "NoteEvents"):
        print("✗ Failed to load noteevents data")
        conn.close()
        sys.exit(1)

    # Verify
    if verify_data(conn):
        print("\n" + "=" * 70)
        print("✓ All data loaded successfully!")
        print("=" * 70)
        print("\nNext step: Run load_to_mongodb.py to migrate to MongoDB")
    else:
        print("\n✗ Verification failed")

    conn.close()

if __name__ == "__main__":
    main()
