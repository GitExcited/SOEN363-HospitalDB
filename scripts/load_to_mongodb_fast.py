"""
SOEN363 Phase 2 - Fast MongoDB Loading with Streaming
Extract from PostgreSQL and load directly with batch processing
"""

import psycopg2
from pymongo import MongoClient
from datetime import datetime, date
from decimal import Decimal
import os
import sys

def convert_to_mongo_compatible(obj):
    """Convert Python objects to MongoDB-compatible types"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: convert_to_mongo_compatible(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_mongo_compatible(v) for v in obj]
    return obj

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "admin",
    "password": "admin",
    "database": "hospital_db"
}

MONGO_CONFIG = {
    "host": "localhost",
    "port": 27018,
    "username": "admin",
    "password": "admin",
    "database": "hospital_db_nosql"
}

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def connect_postgres():
    """Connect to PostgreSQL"""
    try:
        print("[POSTGRES] Connecting to PostgreSQL...")
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("✓ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        sys.exit(1)

def connect_mongodb():
    """Connect to MongoDB"""
    try:
        print("[MONGODB] Connecting to MongoDB...")
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[MONGO_CONFIG['database']]
        print("✓ Connected to MongoDB")
        return client, db
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        sys.exit(1)

def load_patients_streaming(postgres_conn, mongo_db):
    """Load patients with streaming approach - build and insert in batches"""
    print("\n[LOAD] Loading patients with streaming approach...")

    cursor = postgres_conn.cursor()
    batch_size = 100
    batch_docs = []
    total_inserted = 0

    try:
        # Get all patients with their basic info
        cursor.execute("SELECT * FROM patients ORDER BY subject_id")
        patients = cursor.fetchall()
        patient_cols = [desc[0] for desc in cursor.description]
        patient_col_idx = {col: i for i, col in enumerate(patient_cols)}
        subject_idx = patient_col_idx['subject_id']

        print(f"  Processing {len(patients)} patients...")

        # Get admissions
        cursor.execute("SELECT * FROM admissions ORDER BY subject_id, hadm_id")
        admissions = cursor.fetchall()
        admission_cols = [desc[0] for desc in cursor.description]
        admission_col_idx = {col: i for i, col in enumerate(admission_cols)}
        adm_subject_idx = admission_col_idx['subject_id']
        adm_hadm_idx = admission_col_idx['hadm_id']

        # Get icustays
        cursor.execute("SELECT * FROM icustays ORDER BY hadm_id")
        icustays = cursor.fetchall()
        icustay_cols = [desc[0] for desc in cursor.description]
        icustay_col_idx = {col: i for i, col in enumerate(icustay_cols)}
        icu_hadm_idx = icustay_col_idx['hadm_id']

        # Get diagnoses
        cursor.execute("SELECT * FROM diagnoses_icd ORDER BY hadm_id")
        diagnoses = cursor.fetchall()
        diagnose_cols = [desc[0] for desc in cursor.description]
        diagnose_col_idx = {col: i for i, col in enumerate(diagnose_cols)}
        diag_hadm_idx = diagnose_col_idx['hadm_id']

        # Get ICD9 titles
        cursor.execute("SELECT * FROM d_icd_diagnoses")
        icd_titles = cursor.fetchall()
        title_cols = [desc[0] for desc in cursor.description]
        title_idx = {col: i for i, col in enumerate(title_cols)}

        titles_by_code = {}
        for row in icd_titles:
            code = row[title_idx['icd9_code']]
            titles_by_code[code] = {
                "short_title": row[title_idx['short_title']],
                "long_title": row[title_idx['long_title']]
            }

        # Index by hadm_id for fast lookup
        admissions_by_subject = {}
        for adm in admissions:
            subj_id = adm[adm_subject_idx]
            if subj_id not in admissions_by_subject:
                admissions_by_subject[subj_id] = []
            admissions_by_subject[subj_id].append(adm)

        icustays_by_hadm = {}
        for icu in icustays:
            hadm_id = icu[icu_hadm_idx]
            if hadm_id not in icustays_by_hadm:
                icustays_by_hadm[hadm_id] = []
            icustays_by_hadm[hadm_id].append(icu)

        diagnoses_by_hadm = {}
        for diag in diagnoses:
            hadm_id = diag[diag_hadm_idx]
            if hadm_id not in diagnoses_by_hadm:
                diagnoses_by_hadm[hadm_id] = []
            diagnoses_by_hadm[hadm_id].append(diag)

        # Process patients in batches
        for i, patient in enumerate(patients):
            subject_id = patient[subject_idx]

            # Build patient document
            patient_doc = {"_id": subject_id}
            for col, idx in patient_col_idx.items():
                if col != 'row_id':
                    patient_doc[col] = patient[idx]

            # Add admissions
            patient_admissions = admissions_by_subject.get(subject_id, [])
            embedded_admissions = []

            for adm in patient_admissions:
                hadm_id = adm[adm_hadm_idx]
                adm_doc = {}
                for col, idx in admission_col_idx.items():
                    if col not in ['row_id', 'subject_id']:
                        adm_doc[col] = adm[idx]

                # Add icustays
                adm_doc['icustays'] = []
                for icu in icustays_by_hadm.get(hadm_id, []):
                    icu_doc = {}
                    for col, idx in icustay_col_idx.items():
                        if col not in ['row_id', 'subject_id', 'hadm_id']:
                            icu_doc[col] = icu[idx]
                    adm_doc['icustays'].append(icu_doc)

                # Add diagnoses
                adm_doc['diagnoses_icd'] = []
                for diag in diagnoses_by_hadm.get(hadm_id, []):
                    diag_doc = {}
                    for col, idx in diagnose_col_idx.items():
                        if col not in ['row_id', 'subject_id', 'hadm_id']:
                            diag_doc[col] = diag[idx]

                    icd_code = diag[diagnose_col_idx['icd9_code']]         
                    if icd_code in titles_by_code:
                        diag_doc['short_title'] = titles_by_code[icd_code]['short_title']
                        diag_doc['long_title'] = titles_by_code[icd_code]['long_title']
                    else:
                        diag_doc['short_title'] = None
                        diag_doc['long_title'] = None
                    adm_doc['diagnoses_icd'].append(diag_doc)

                embedded_admissions.append(adm_doc)

            patient_doc['admissions'] = embedded_admissions
            batch_docs.append(convert_to_mongo_compatible(patient_doc))

            # Insert batch when ready
            if len(batch_docs) >= batch_size:
                mongo_db['patients'].insert_many(batch_docs, ordered=False)
                total_inserted += len(batch_docs)
                if (i + 1) % 10000 == 0:
                    print(f"  ✓ Processed {i + 1}/{len(patients)} patients ({total_inserted} inserted)")
                batch_docs = []

        # Insert remaining
        if batch_docs:
            mongo_db['patients'].insert_many(batch_docs, ordered=False)
            total_inserted += len(batch_docs)

        print(f"  ✓ Inserted {total_inserted} patient documents")
        cursor.close()
        return total_inserted

    except Exception as e:
        print(f"✗ Error loading patients: {e}")
        cursor.close()
        return 0

def load_noteevents_streaming(postgres_conn, mongo_db):
    """Load noteevents with streaming"""
    print("\n[LOAD] Loading noteevents...")

    cursor = postgres_conn.cursor()
    batch_size = 500
    batch_docs = []
    total_inserted = 0

    try:
        cursor.execute("SELECT * FROM noteevents ORDER BY subject_id")
        cols = [desc[0] for desc in cursor.description]
        col_idx = {col: i for i, col in enumerate(cols)}

        row_count = 0
        for note_row in cursor:
            row_count += 1
            note_doc = {}
            for col, idx in col_idx.items():
                if col != 'row_id':
                    note_doc[col] = note_row[idx]
            batch_docs.append(convert_to_mongo_compatible(note_doc))

            if len(batch_docs) >= batch_size:
                mongo_db['noteevents'].insert_many(batch_docs, ordered=False)
                total_inserted += len(batch_docs)
                if row_count % 10000 == 0:
                    print(f"  ✓ Processed {row_count} noteevents ({total_inserted} inserted)")
                batch_docs = []

        if batch_docs:
            mongo_db['noteevents'].insert_many(batch_docs, ordered=False)
            total_inserted += len(batch_docs)

        print(f"  ✓ Inserted {total_inserted} noteevent documents")
        cursor.close()
        return total_inserted

    except Exception as e:
        print(f"✗ Error loading noteevents: {e}")
        cursor.close()
        return 0

def create_indexes(mongo_db):
    """Create indexes"""
    print("\n[INDEXES] Creating indexes...")
    try:
        mongo_db['patients'].create_index([('subject_id', 1)])
        mongo_db['patients'].create_index([('gender', 1)])
        mongo_db['noteevents'].create_index([('subject_id', 1)])
        mongo_db['noteevents'].create_index([('hadm_id', 1)])
        print("  ✓ Created 4 indexes")
    except Exception as e:
        print(f"⚠ Error creating indexes: {e}")

def verify_data(mongo_db):
    """Verify data"""
    try:
        print("\n[VERIFY] Verifying MongoDB...")
        patient_count = mongo_db['patients'].count_documents({})
        noteevent_count = mongo_db['noteevents'].count_documents({})
        print(f"  ✓ Patients: {patient_count:,}")
        print(f"  ✓ NoteEvents: {noteevent_count:,}")
        return True
    except Exception as e:
        print(f"✗ Verification failed: {e}")
        return False

def main():
    print("=" * 70)
    print("SOEN363 PHASE 2 - FAST MONGODB LOADING")
    print("Stream from PostgreSQL, batch insert to MongoDB")
    print("=" * 70)

    start_time = datetime.now()

    postgres_conn = connect_postgres()
    mongo_client, mongo_db = connect_mongodb()

    try:
        # Clear collections
        mongo_db['patients'].drop()
        mongo_db['noteevents'].drop()
        print("\n✓ Cleared existing collections")

        # Load patients
        patients_count = load_patients_streaming(postgres_conn, mongo_db)

        # Load noteevents
        noteevents_count = load_noteevents_streaming(postgres_conn, mongo_db)

        # Create indexes
        create_indexes(mongo_db)

        # Verify
        if verify_data(mongo_db):
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print("\n" + "=" * 70)
            print("✓ MongoDB migration completed successfully!")
            print(f"  Total time: {duration:.2f} seconds ({duration/60:.1f} minutes)")
            print("=" * 70)

    except Exception as e:
        print(f"✗ Error: {e}")

    finally:
        postgres_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    main()
