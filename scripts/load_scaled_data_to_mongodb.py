"""
SOEN363 Phase 2 - Load Scaled Data to MongoDB
Person 4: Load large JSON files (279K patients + 139K notes) into MongoDB
"""

import json
import sys
from pymongo import MongoClient
from datetime import datetime
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

MONGO_CONFIG = {
    "host": "localhost",
    "port": 27018,
    "username": "admin",
    "password": "admin",
    "database": "hospital_db_nosql"
}

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
        print("SCALED DATA LOADING EXECUTION REPORT")
        print("=" * 70)
        print(f"Execution Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration:.2f} seconds")
        print()
        print("Insertion Statistics:")
        print(f"  - Patients documents inserted: {self.stats['patients_inserted']:,}")
        print(f"  - NoteEvents documents inserted: {self.stats['noteevents_inserted']:,}")
        print(f"  - Total admissions embedded: {self.stats['total_admissions']:,}")
        print(f"  - Total ICU stays embedded: {self.stats['total_icustays']:,}")
        print(f"  - Total diagnoses embedded: {self.stats['total_diagnoses']:,}")
        print()
        print("Indexes Created:")
        for idx in self.stats["indexes_created"]:
            print(f"  - {idx}")

        if self.stats["errors"]:
            print()
            print(f"Errors encountered: {len(self.stats['errors'])}")
            for err in self.stats["errors"][:5]:  # Show first 5 errors
                print(f"  - {err}")

        print("=" * 70)

        return self.stats

    def save_report(self, filepath):
        """Save the report to a text file"""
        with open(filepath, 'w') as f:
            f.write("=" * 70 + "\n")
            f.write("SCALED DATA LOADING EXECUTION REPORT\n")
            f.write("=" * 70 + "\n")
            f.write(f"Execution Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Duration: {(datetime.now() - self.start_time).total_seconds():.2f} seconds\n\n")
            f.write("Insertion Statistics:\n")
            f.write(f"  - Patients documents inserted: {self.stats['patients_inserted']:,}\n")
            f.write(f"  - NoteEvents documents inserted: {self.stats['noteevents_inserted']:,}\n")
            f.write(f"  - Total admissions embedded: {self.stats['total_admissions']:,}\n")
            f.write(f"  - Total ICU stays embedded: {self.stats['total_icustays']:,}\n")
            f.write(f"  - Total diagnoses embedded: {self.stats['total_diagnoses']:,}\n\n")
            f.write("Indexes Created:\n")
            for idx in self.stats["indexes_created"]:
                f.write(f"  - {idx}\n")

            if self.stats["errors"]:
                f.write(f"\nErrors encountered: {len(self.stats['errors'])}\n")
                for err in self.stats["errors"][:10]:
                    f.write(f"  - {err}\n")

            f.write("=" * 70 + "\n")


# ============================================================================
# DATABASE CONNECTIONS
# ============================================================================

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
# DATA LOADING FROM JSON FILES
# ============================================================================

def load_patients_from_json(json_file):
    """Load patients from JSON file"""
    try:
        print(f"\n[LOAD] Reading patients from JSON file: {json_file}")

        with open(json_file, 'r') as f:
            data = json.load(f)

        # Handle different JSON formats
        if isinstance(data, list):
            patients_data = data
        elif isinstance(data, dict) and 'jsonb_agg' in data:
            patients_data = data['jsonb_agg']
        else:
            patients_data = list(data.values()) if isinstance(data, dict) else []

        print(f"✓ Loaded {len(patients_data):,} patients from JSON")
        return patients_data

    except Exception as e:
        print(f"✗ Error loading patients JSON: {e}")
        return []


def load_noteevents_from_json(json_file):
    """Load note events from JSON file"""
    try:
        print(f"[LOAD] Reading noteevents from JSON file: {json_file}")

        with open(json_file, 'r') as f:
            data = json.load(f)

        # Handle different JSON formats
        if isinstance(data, list):
            noteevents_data = data
        elif isinstance(data, dict) and 'jsonb_agg' in data:
            noteevents_data = data['jsonb_agg']
        else:
            noteevents_data = list(data.values()) if isinstance(data, dict) else []

        print(f"✓ Loaded {len(noteevents_data):,} note events from JSON")
        return noteevents_data

    except Exception as e:
        print(f"✗ Error loading noteevents JSON: {e}")
        return []


def insert_patients_to_mongodb(mongo_db, patients_data, report):
    """Insert patient documents into MongoDB"""
    try:
        print("\n[INSERT] Inserting patients into MongoDB...")
        patients_collection = mongo_db['patients']

        # Count embedded documents
        total_inserted = 0
        batch_size = 1000  # Insert in batches for better performance

        for i in range(0, len(patients_data), batch_size):
            batch = patients_data[i:i+batch_size]

            # Prepare documents
            docs_to_insert = []
            for patient in batch:
                doc = {
                    "_id": patient.get("subject_id"),
                    "gender": patient.get("gender"),
                    "dob": patient.get("dob"),
                    "dod": patient.get("dod"),
                    "dod_hosp": patient.get("dod_hosp"),
                    "dod_ssn": patient.get("dod_ssn"),
                    "expire_flag": patient.get("expire_flag"),
                    "admissions": patient.get("admissions", [])
                }
                docs_to_insert.append(doc)

                # Count embedded data
                if patient.get("admissions"):
                    report.stats["total_admissions"] += len(patient.get("admissions", []))
                    for adm in patient.get("admissions", []):
                        report.stats["total_icustays"] += len(adm.get("icustays", []))
                        report.stats["total_diagnoses"] += len(adm.get("diagnoses_icd", []))

            # Insert batch
            try:
                result = patients_collection.insert_many(docs_to_insert, ordered=False)
                total_inserted += len(result.inserted_ids)
            except Exception as e:
                # Some duplicates are okay, count what was inserted
                if "duplicate" in str(e).lower():
                    report.add_error(f"Some duplicate patients in batch {i//batch_size + 1} (expected if re-running)")
                else:
                    raise

            # Progress indicator
            if (i // batch_size + 1) % 10 == 0:
                print(f"  Progress: {i + batch_size:,} / {len(patients_data):,} patients...")

        report.stats["patients_inserted"] = total_inserted
        print(f"✓ Inserted {total_inserted:,} patient documents")

    except Exception as e:
        report.add_error(f"Error inserting patients: {str(e)[:200]}")


def insert_noteevents_to_mongodb(mongo_db, noteevents_data, report):
    """Insert note event documents into MongoDB"""
    try:
        print("[INSERT] Inserting noteevents into MongoDB...")
        noteevents_collection = mongo_db['noteevents']

        total_inserted = 0
        batch_size = 5000  # Larger batch for notes

        for i in range(0, len(noteevents_data), batch_size):
            batch = noteevents_data[i:i+batch_size]

            try:
                result = noteevents_collection.insert_many(batch, ordered=False)
                total_inserted += len(result.inserted_ids)
            except Exception as e:
                if "duplicate" in str(e).lower():
                    report.add_error(f"Some duplicate notes in batch {i//batch_size + 1} (expected if re-running)")
                else:
                    raise

            # Progress indicator
            if (i // batch_size + 1) % 5 == 0:
                print(f"  Progress: {i + batch_size:,} / {len(noteevents_data):,} notes...")

        report.stats["noteevents_inserted"] = total_inserted
        print(f"✓ Inserted {total_inserted:,} note event documents")

    except Exception as e:
        report.add_error(f"Error inserting noteevents: {str(e)[:200]}")


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
        patients_collection.create_index("subject_id")
        report.stats["indexes_created"].append("patients.subject_id")

        patients_collection.create_index("admissions.hadm_id")
        report.stats["indexes_created"].append("patients.admissions.hadm_id")

        patients_collection.create_index("expire_flag")
        report.stats["indexes_created"].append("patients.expire_flag")

        # NoteEvents indexes
        noteevents_collection.create_index("subject_id")
        report.stats["indexes_created"].append("noteevents.subject_id")

        noteevents_collection.create_index("hadm_id")
        report.stats["indexes_created"].append("noteevents.hadm_id")

        noteevents_collection.create_index("chartdate")
        report.stats["indexes_created"].append("noteevents.chartdate")

        print(f"✓ Created {len(report.stats['indexes_created'])} indexes")

    except Exception as e:
        report.add_error(f"Error creating indexes: {str(e)}")


# ============================================================================
# VALIDATION
# ============================================================================

def validate_and_report_mongodb(mongo_db, report):
    """Validate loaded data and report statistics"""
    try:
        print("\n[VALIDATION] Validating MongoDB data...")

        patients_count = mongo_db['patients'].count_documents({})
        noteevents_count = mongo_db['noteevents'].count_documents({})

        print(f"✓ Patients collection: {patients_count:,} documents")
        print(f"✓ NoteEvents collection: {noteevents_count:,} documents")

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
    print("SOEN363 PHASE 2 - LOAD SCALED DATA TO MONGODB")
    print("Person 4: Loading 279K Patients + 139K Notes")
    print("=" * 70)

    # Initialize report
    report = DataLoadingReport()

    # Connect to MongoDB
    mongo_client = connect_to_mongodb()
    mongo_db = mongo_client[MONGO_CONFIG['database']]

    # Define file paths
    patients_json = os.path.join(PROJECT_ROOT, "scaled_JSON_output.json")
    noteevents_json = os.path.join(PROJECT_ROOT, "scaled_JSON_output_notes.json")

    try:
        # Load data from JSON files
        patients_data = load_patients_from_json(patients_json)
        noteevents_data = load_noteevents_from_json(noteevents_json)

        # Insert into MongoDB
        if patients_data:
            insert_patients_to_mongodb(mongo_db, patients_data, report)
        else:
            report.add_error("No patient data loaded from JSON")

        if noteevents_data:
            insert_noteevents_to_mongodb(mongo_db, noteevents_data, report)
        else:
            report.add_error("No noteevents data loaded from JSON")

        # Create indexes
        create_indexes(mongo_db, report)

        # Validate
        validate_and_report_mongodb(mongo_db, report)

        # Print and save report
        print()
        report.print_report()

        report_file = os.path.join(PROJECT_ROOT, "..", "reports", "scaled_data_loading_report.txt")
        os.makedirs(os.path.dirname(report_file), exist_ok=True)
        report.save_report(report_file)
        print(f"\n✓ Report saved to: {report_file}")

    except Exception as e:
        report.add_error(f"Critical error in main execution: {str(e)}")
        report.print_report()

    finally:
        mongo_client.close()
        print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()
