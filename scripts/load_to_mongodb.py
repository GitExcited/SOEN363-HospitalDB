"""
SOEN363 Phase 2 - Data Loading to MongoDB
Person 4: Load transformed JSON data into MongoDB
Follows the NoSQL schema design from Person 2 (David)
"""

import json
import psycopg2
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from datetime import datetime
import os
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

# PostgreSQL connection
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "admin",
    "password": "admin",
    "database": "hospital_db"
}

# MongoDB connection
MONGO_CONFIG = {
    "host": "localhost",
    "port": 27018,
    "username": "admin",
    "password": "admin",
    "database": "hospital_db_nosql"
}

# Project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ============================================================================
# LOGGING & REPORTING
# ============================================================================

class DataLoadingReport:
    def __init__(self):
        self.start_time = datetime.now()
        self.stats = {
            "patients_inserted": 0,
            "noteevents_inserted": 0,
            "total_admissions": 0,
            "total_icustays": 0,
            "total_diagnoses": 0,
            "indexes_created": [],
            "errors": []
        }

    def add_error(self, error_msg):
        self.stats["errors"].append(error_msg)
        print(f"⚠ ERROR: {error_msg}")

    def print_report(self):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        print("\n" + "=" * 70)
        print("DATA LOADING EXECUTION REPORT")
        print("=" * 70)
        print(f"Execution Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration:.2f} seconds")
        print()
        print("Insertion Statistics:")
        print(f"  - Patients documents inserted: {self.stats['patients_inserted']}")
        print(f"  - NoteEvents documents inserted: {self.stats['noteevents_inserted']}")
        print(f"  - Total admissions embedded: {self.stats['total_admissions']}")
        print(f"  - Total ICU stays embedded: {self.stats['total_icustays']}")
        print(f"  - Total diagnoses embedded: {self.stats['total_diagnoses']}")
        print()
        print("Indexes Created:")
        for idx in self.stats["indexes_created"]:
            print(f"  - {idx}")

        if self.stats["errors"]:
            print()
            print(f"Errors encountered: {len(self.stats['errors'])}")
            for err in self.stats["errors"]:
                print(f"  - {err}")

        print("=" * 70)

        return self.stats

    def save_report(self, filepath):
        """Save the report to a text file"""
        with open(filepath, 'w') as f:
            f.write("=" * 70 + "\n")
            f.write("DATA LOADING EXECUTION REPORT\n")
            f.write("=" * 70 + "\n")
            f.write(f"Execution Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Duration: {(datetime.now() - self.start_time).total_seconds():.2f} seconds\n\n")
            f.write("Insertion Statistics:\n")
            f.write(f"  - Patients documents inserted: {self.stats['patients_inserted']}\n")
            f.write(f"  - NoteEvents documents inserted: {self.stats['noteevents_inserted']}\n")
            f.write(f"  - Total admissions embedded: {self.stats['total_admissions']}\n")
            f.write(f"  - Total ICU stays embedded: {self.stats['total_icustays']}\n")
            f.write(f"  - Total diagnoses embedded: {self.stats['total_diagnoses']}\n\n")
            f.write("Indexes Created:\n")
            for idx in self.stats["indexes_created"]:
                f.write(f"  - {idx}\n")

            if self.stats["errors"]:
                f.write(f"\nErrors encountered: {len(self.stats['errors'])}\n")
                for err in self.stats["errors"]:
                    f.write(f"  - {err}\n")

            f.write("=" * 70 + "\n")


# ============================================================================
# DATABASE CONNECTIONS
# ============================================================================

def connect_to_postgres():
    """Connect to PostgreSQL database"""
    try:
        print("\n[POSTGRES] Connecting to PostgreSQL...")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("✓ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)


def connect_to_mongodb():
    """Connect to MongoDB database"""
    try:
        print("[MONGODB] Connecting to MongoDB...")
        connection_uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/?authSource=admin"
        client = MongoClient(connection_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✓ Connected to MongoDB")
        return client
    except Exception as e:
        print(f"✗ Failed to connect to MongoDB: {e}")
        sys.exit(1)


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_patients_from_postgres(postgres_conn):
    """
    Extract patient data from PostgreSQL using the prepared query
    This query returns fully denormalized patient documents with embedded admissions
    """
    try:
        print("\n[EXTRACT] Executing patient export query...")
        cursor = postgres_conn.cursor()

        # Read the SQL query from file (check multiple locations)
        query_file_locations = [
            os.path.join(PROJECT_ROOT, "person3", "export_query_PatientDocuments.sql"),
            os.path.join(PROJECT_ROOT, "export_query_PatientDocuments.sql"),
        ]

        query_file = None
        for location in query_file_locations:
            if os.path.exists(location):
                query_file = location
                break

        if not query_file:
            raise FileNotFoundError("export_query_PatientDocuments.sql not found in expected locations")

        with open(query_file, 'r') as f:
            query = f.read()

        cursor.execute(query)
        result = cursor.fetchone()

        if result and result[0]:
            patients_data = result[0]  # This is already a JSON array from PostgreSQL
            print(f"✓ Extracted data for {len(patients_data)} patients")
            return patients_data
        else:
            print("⚠ No patient data extracted")
            return []

    except Exception as e:
        print(f"✗ Error extracting patients: {e}")
        return []
    finally:
        cursor.close()


def extract_noteevents_from_postgres(postgres_conn):
    """
    Extract note events from PostgreSQL using the prepared query
    """
    try:
        print("[EXTRACT] Executing noteevents export query...")
        cursor = postgres_conn.cursor()

        # Read the SQL query from file (check multiple locations)
        query_file_locations = [
            os.path.join(PROJECT_ROOT, "person3", "export_query_NoteEvents.sql"),
            os.path.join(PROJECT_ROOT, "sql", "export_query_NoteEvents.sql"),
        ]

        query_file = None
        for location in query_file_locations:
            if os.path.exists(location):
                query_file = location
                break

        if not query_file:
            raise FileNotFoundError("export_query_NoteEvents.sql not found in expected locations")

        with open(query_file, 'r') as f:
            query = f.read()

        cursor.execute(query)
        result = cursor.fetchone()

        if result and result[0]:
            noteevents_data = result[0]  # This is already a JSON array from PostgreSQL
            print(f"✓ Extracted data for {len(noteevents_data)} note events")
            return noteevents_data
        else:
            print("⚠ No noteevents data extracted")
            return []

    except Exception as e:
        print(f"✗ Error extracting noteevents: {e}")
        return []
    finally:
        cursor.close()


# ============================================================================
# DATA LOADING
# ============================================================================

def load_patients_to_mongodb(mongo_db, patients_data, report):
    """
    Load patient documents into MongoDB
    Documents structure:
    {
        "_id": subject_id,
        "gender": "...",
        "dob": ISODate(...),
        "dod": ISODate(...),
        "expire_flag": ...,
        "admissions": [
            {
                "hadm_id": ...,
                "icustays": [...],
                "diagnoses_icd": [...]
            }
        ]
    }
    """
    try:
        print("\n[LOAD] Loading patients collection...")
        patients_collection = mongo_db['patients']

        # Transform and insert patients
        patients_to_insert = []

        for patient in patients_data:
            # Prepare the patient document
            patient_doc = {
                "_id": patient.get("subject_id"),
                "gender": patient.get("gender"),
                "dob": patient.get("dob"),
                "dod": patient.get("dod"),
                "dod_hosp": patient.get("dod_hosp"),
                "dod_ssn": patient.get("dod_ssn"),
                "expire_flag": patient.get("expire_flag"),
                "admissions": patient.get("admissions", [])
            }

            patients_to_insert.append(patient_doc)

            # Count embedded documents
            if patient.get("admissions"):
                report.stats["total_admissions"] += len(patient.get("admissions", []))

                for admission in patient.get("admissions", []):
                    report.stats["total_icustays"] += len(admission.get("icustays", []))
                    report.stats["total_diagnoses"] += len(admission.get("diagnoses_icd", []))

        # Batch insert
        if patients_to_insert:
            result = patients_collection.insert_many(patients_to_insert, ordered=False)
            report.stats["patients_inserted"] = len(result.inserted_ids)
            print(f"✓ Loaded {len(result.inserted_ids)} patient documents")

    except Exception as e:
        report.add_error(f"Error loading patients: {str(e)}")


def load_noteevents_to_mongodb(mongo_db, noteevents_data, report):
    """
    Load note events into MongoDB as separate collection
    Documents structure:
    {
        "_id": note_id (generated),
        "subject_id": ...,
        "hadm_id": ...,
        "chartdate": ISODate(...),
        "charttime": ISODate(...),
        ...
    }
    """
    try:
        print("[LOAD] Loading noteevents collection...")
        noteevents_collection = mongo_db['noteevents']

        # Transform noteevents - MongoDB will auto-generate _id if not provided
        noteevents_to_insert = []

        for note in noteevents_data:
            note_doc = {
                "subject_id": note.get("subject_id"),
                "hadm_id": note.get("hadm_id"),
                "chartdate": note.get("chartdate"),
                "charttime": note.get("charttime"),
                "storetime": note.get("storetime"),
                "category": note.get("category"),
                "description": note.get("description"),
                "cgid": note.get("cgid"),
                "iserror": note.get("iserror"),
                "text": note.get("text")
            }
            noteevents_to_insert.append(note_doc)

        # Batch insert
        if noteevents_to_insert:
            result = noteevents_collection.insert_many(noteevents_to_insert, ordered=False)
            report.stats["noteevents_inserted"] = len(result.inserted_ids)
            print(f"✓ Loaded {len(result.inserted_ids)} note event documents")

    except Exception as e:
        report.add_error(f"Error loading noteevents: {str(e)}")


# ============================================================================
# INDEX CREATION
# ============================================================================

def create_indexes(mongo_db, report):
    """Create indexes on commonly queried fields"""
    try:
        print("\n[INDEXES] Creating database indexes...")

        patients_collection = mongo_db['patients']
        noteevents_collection = mongo_db['noteevents']

        # Patients indexes
        # Index on subject_id (used for lookups)
        patients_collection.create_index("subject_id")
        report.stats["indexes_created"].append("patients.subject_id")

        # Index on admissions.hadm_id (for finding admissions)
        patients_collection.create_index("admissions.hadm_id")
        report.stats["indexes_created"].append("patients.admissions.hadm_id")

        # Index on expire_flag (for filtering)
        patients_collection.create_index("expire_flag")
        report.stats["indexes_created"].append("patients.expire_flag")

        # NoteEvents indexes
        # Index on subject_id (for patient-related queries)
        noteevents_collection.create_index("subject_id")
        report.stats["indexes_created"].append("noteevents.subject_id")

        # Index on hadm_id (for admission-related queries)
        noteevents_collection.create_index("hadm_id")
        report.stats["indexes_created"].append("noteevents.hadm_id")

        # Index on chartdate (for time-based queries)
        noteevents_collection.create_index("chartdate")
        report.stats["indexes_created"].append("noteevents.chartdate")

        print(f"✓ Created {len(report.stats['indexes_created'])} indexes")

    except Exception as e:
        report.add_error(f"Error creating indexes: {str(e)}")


# ============================================================================
# VALIDATION & STATISTICS
# ============================================================================

def validate_and_report_mongodb(mongo_db, report):
    """Validate loaded data and report statistics"""
    try:
        print("\n[VALIDATION] Validating MongoDB data...")

        patients_count = mongo_db['patients'].count_documents({})
        noteevents_count = mongo_db['noteevents'].count_documents({})

        print(f"✓ Patients collection: {patients_count} documents")
        print(f"✓ NoteEvents collection: {noteevents_count} documents")

        # Get database statistics
        db_stats = mongo_db.command("dbStats")
        db_size_gb = db_stats.get('dataSize', 0) / (1024 ** 3)

        print(f"✓ Database size: {db_size_gb:.2f} GB")

    except Exception as e:
        report.add_error(f"Error during validation: {str(e)}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("\n" + "=" * 70)
    print("SOEN363 PHASE 2 - DATA LOADING TO MONGODB")
    print("Person 4: Data Loading & Transformation")
    print("=" * 70)

    # Initialize report
    report = DataLoadingReport()

    # Connect to databases
    postgres_conn = connect_to_postgres()
    mongo_client = connect_to_mongodb()
    mongo_db = mongo_client[MONGO_CONFIG['database']]

    try:
        # Extract data from PostgreSQL
        patients_data = extract_patients_from_postgres(postgres_conn)
        noteevents_data = extract_noteevents_from_postgres(postgres_conn)

        # Load data into MongoDB
        if patients_data:
            load_patients_to_mongodb(mongo_db, patients_data, report)
        else:
            report.add_error("No patient data available for loading")

        if noteevents_data:
            load_noteevents_to_mongodb(mongo_db, noteevents_data, report)
        else:
            print("⚠ No noteevents data available (this is optional)")

        # Create indexes
        create_indexes(mongo_db, report)

        # Validate and report
        validate_and_report_mongodb(mongo_db, report)

        # Print final report
        print()
        report.print_report()

        # Save report to file
        report_file = os.path.join(PROJECT_ROOT, "..", "reports", "loading_execution_report.txt")
        os.makedirs(os.path.dirname(report_file), exist_ok=True)
        report.save_report(report_file)
        print(f"\n✓ Report saved to: {report_file}")

    except Exception as e:
        report.add_error(f"Critical error in main execution: {str(e)}")
        report.print_report()

    finally:
        # Clean up connections
        postgres_conn.close()
        mongo_client.close()
        print("\n✓ Database connections closed")


if __name__ == "__main__":
    main()
