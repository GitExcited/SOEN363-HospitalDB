#!/usr/bin/env python3
"""
SOEN363 Phase 2 - Validation Script (Person 5)

Validates that PostgreSQL data is correctly migrated into MongoDB.

Mongo design assumed from export_query_PatientDocuments.sql:
- patients collection
  - admissions: [ { ...,
      icustays: [ ... ],
      diagnoses_icd: [ ... ]
    } ]

Separate collection assumed from export_query_NoteEvents.sql:
- noteevents collection (flat)

Outputs: reports/validation_report.txt
"""

import os
import random
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from pymongo import MongoClient

POSTGRES_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DB", "hospital_db"),
    "user": os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:admin@localhost:27018/?authSource=admin"
)
MONGO_DB_NAME = os.getenv("MONGO_DB", "hospital_db_nosql") 

def log(lines, s=""):
    print(s)
    lines.append(s)

def connect_to_postgres():
    print("[POSTGRES] Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("Connection succesful")
        return conn
    except Exception as e:
        print(f"Connection Failed: {e}")
        exit(1)

def connect_to_mongo():
    print("[MONGO] Connecting to MongoDB...")
    try:
        client = MongoClient(MONGO_URI)
        print("Connection succesful")
        return client[MONGO_DB_NAME]
    except Exception as e:
        print(f"Connection Failed: {e}")
        exit(1)

def fetch_patients_count_postgres(pg_connection):
    cursor = pg_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM patients;")
    return cursor.fetchone()[0]

def fetch_admission_count_postgres(pg_connection):
    cursor = pg_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM admissions;")
    return cursor.fetchone()[0]

def fetch_diagnosis_count_postgres(pg_connection):
    cursor = pg_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM diagnoses_icd;")
    return cursor.fetchone()[0]

def fetch_icu_stay_count_postgres(pg_connection):
    cursor = pg_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM icustays;")
    return cursor.fetchone()[0]

def fetch_note_event_count_postgres(pg_connection):
    cursor = pg_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM noteevents;")
    return cursor.fetchone()[0]

def fetch_patients_count_mongo(mongo):
    return mongo["patients"].count_documents({})

def fetch_admission_count_mongo(mongo):
    pipeline = [
        {"$unwind": "$admissions"},
        {"$count": "total"}
    ]
    result = list(mongo["patients"].aggregate(pipeline))
    return result[0]["total"] if result else 0

def fetch_icu_stay_count_mongo(mongo):
    pipeline = [
        {"$unwind": "$admissions"},
        {"$unwind": "$admissions.icustays"},
        {"$count": "total"}
    ]
    result = list(mongo["patients"].aggregate(pipeline))
    return result[0]["total"] if result else 0

def fetch_diagnosis_count_mongo(mongo):
    pipeline = [
        {"$unwind": "$admissions"},
        {"$unwind": "$admissions.diagnoses_icd"},
        {"$count": "total"}
    ]
    result = list(mongo["patients"].aggregate(pipeline))
    return result[0]["total"] if result else 0

def fetch_note_event_count_mongo(mongo):
    return mongo["noteevents"].count_documents({})

def compare_count(postgres_patients, mongo_patients):
    
    if postgres_patients == mongo_patients:
        print("[Pass]")
    else:
        print("[Fail] Count is not the same")
        exit(1)

def validate_patient_count(postgres_connection, mongo_connection):
    print("="*40)
    print("Verifying patient count\n")
    postgres_start_time = time.perf_counter()
    postgres_patient_count = fetch_patients_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    print(f"{postgres_patient_count} patients found in SQL DB")
    print(f"({postgres_elapsed_time}) seconds")
    print("\n")

    mongo_start_time = time.perf_counter()
    mongo_patient_count = fetch_patients_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    print(f"{mongo_patient_count} patients found in NOSQL DB")
    print(f"({mongo_elapsed_time}) seconds")
    print("\n")
    compare_count(postgres_patient_count , mongo_patient_count)

def validate_admission_count(postgres_connection, mongo_connection):
    print("="*40)
    print("Verifying admission count\n")
    postgres_start_time = time.perf_counter()
    postgres_admission_count = fetch_admission_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    print(f"{postgres_admission_count} admmissions found in SQL DB")
    print(f"({postgres_elapsed_time}) seconds")
    print("\n")

    mongo_start_time = time.perf_counter()
    mongo_admission_count = fetch_admission_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    print(f"{mongo_admission_count} admissions found in NOSQL DB")
    print(f"({mongo_elapsed_time}) seconds")
    print("\n")
    compare_count(postgres_admission_count , mongo_admission_count)

def validate_diagnosis_count(postgres_connection, mongo_connection):
    print("="*40)
    print("Verifying diagnosis count\n")
    postgres_start_time = time.perf_counter()
    postgres_diagnosis_count = fetch_diagnosis_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    print(f"{postgres_diagnosis_count} diagnoses found in SQL DB")
    print(f"({postgres_elapsed_time}) seconds")
    print("\n")

    mongo_start_time = time.perf_counter()
    mongo_diagnosis_count = fetch_diagnosis_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    print(f"{mongo_diagnosis_count} diagnoses found in NOSQL DB")
    print(f"({mongo_elapsed_time}) seconds")
    print("\n")
    compare_count(postgres_diagnosis_count , mongo_diagnosis_count)

def validate_icu_stay_count(postgres_connection, mongo_connection):
    print("="*40)
    print("Verifying icu stay count\n")
    postgres_start_time = time.perf_counter()
    postgres_icu_stay_count = fetch_icu_stay_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    print(f"{postgres_icu_stay_count} icu stays found in SQL DB")
    print(f"({postgres_elapsed_time}) seconds")
    print("\n")

    mongo_start_time = time.perf_counter()
    mongo_icu_stay_count = fetch_icu_stay_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    print(f"{mongo_icu_stay_count} icu stays found in NOSQL DB")
    print(f"({mongo_elapsed_time}) seconds")
    print("\n")
    compare_count(postgres_icu_stay_count , mongo_icu_stay_count)

def validate_note_event_count(postgres_connection, mongo_connection):
    print("="*40)
    print("Verifying note event count\n")
    postgres_start_time = time.perf_counter()
    postgres_note_event_count = fetch_note_event_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    print(f"{postgres_note_event_count} note events found in SQL DB")
    print(f"({postgres_elapsed_time}) seconds")
    print("\n")

    mongo_start_time = time.perf_counter()
    mongo_note_event_count = fetch_note_event_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    print(f"{mongo_note_event_count} patients found in NOSQL DB")
    print(f"({mongo_elapsed_time}) seconds")
    print("\n")
    compare_count(postgres_note_event_count , mongo_note_event_count)


def validate_counts(postgres_connection, mongo_connection):
    validate_patient_count(postgres_connection, mongo_connection)
    validate_admission_count(postgres_connection, mongo_connection)
    validate_diagnosis_count(postgres_connection, mongo_connection)
    validate_icu_stay_count(postgres_connection, mongo_connection)
    validate_note_event_count(postgres_connection, mongo_connection)

def fetch_postgres_patients_random(postgres_connection, count):
    cursor = postgres_connection.cursor()
    cursor.execute("SELECT * FROM patients TABLESAMPLE SYSTEM (1) LIMIT (%s);", (count,))
    return cursor.fetchall()


def postgres_count_admissions_for_patient(pg, subject_id):
    cur = pg.cursor()
    cur.execute("SELECT COUNT(*) FROM admissions WHERE subject_id = %s;", (subject_id,))
    return cur.fetchone()[0]

def postgres_get_hadm_ids_for_patient(pg, subject_id):
    cur = pg.cursor()
    cur.execute("SELECT hadm_id FROM admissions WHERE subject_id = %s;", (subject_id,))
    return [row[0] for row in cur.fetchall()]

def postgres_count_icustays_for_hadm(pg, hadm_id):
    cur = pg.cursor()
    cur.execute("SELECT COUNT(*) FROM icustays WHERE hadm_id = %s;", (hadm_id,))
    return cur.fetchone()[0]

def postgres_count_diagnoses_for_hadm(pg, hadm_id):
    cur = pg.cursor()
    cur.execute("SELECT COUNT(*) FROM diagnoses_icd WHERE hadm_id = %s;", (hadm_id,))
    return cur.fetchone()[0]

def validate_nesting(postgres, mongo):
    print("="*40)
    print("Validating Nesting in NO SQL DB")

    sample_size = 100
    sample_postgres_patients = fetch_postgres_patients_random(postgres, sample_size)
    sample_subject_ids = [row[1] for row in sample_postgres_patients]

    if sample_subject_ids:
        print(f"Selected a random sample of {sample_size} patients in SQL DB")
        print("\n")
    else:
        print("Erorr: no patients could be sampled from Postgres — verify DB.")
        exit(1)

    failures = 0

    
    for sid in sample_subject_ids:

        postgres_admission_count = postgres_count_admissions_for_patient(postgres, sid)
        hadm_ids = postgres_get_hadm_ids_for_patient(postgres, sid)
        print(f"found {postgres_admission_count} admissions in SQL DB, corresponding to sample patient: subject_id={sid}")

        postgres_icu_stay_total = 0
        postgres_diagnosis_total = 0
        for hadm in hadm_ids:
            postgres_icu_stay_total += postgres_count_icustays_for_hadm(postgres, hadm)
            postgres_diagnosis_total += postgres_count_diagnoses_for_hadm(postgres, hadm)

        print(f"found {postgres_icu_stay_total} icu_stays in SQL DB, corresponding to sample patient: subject_id={sid}")
        print(f"found {postgres_diagnosis_total} diagnoses in SQL DB, corresponding to sample patient: subject_id={sid}") 


        print("\n")
        print("Validating that all fields were properly nested in NO SQL DB...")

        doc = mongo["patients"].find_one(
            {"subject_id": sid},
            {"admissions": 1, "_id": 0}
        )

        if not doc:
            failures += 1
            print(f"[FAIL] subject_id={sid}: missing patient doc in Mongo")
            continue

        admissions = doc.get("admissions", [])
        mongo_adm_count = len(admissions)

        mongo_icu_total = 0
        mongo_diag_total = 0
        for adm_doc in admissions:
            mongo_icu_total += len(adm_doc.get("icustays", []))
            mongo_diag_total += len(adm_doc.get("diagnoses_icd", []))


        if (postgres_admission_count != mongo_adm_count or
            postgres_icu_stay_total != mongo_icu_total or
            postgres_diagnosis_total != mongo_diag_total):
            failures += 1
            print(
                f"[FAIL] subject_id={sid} "
                f"admissions(pg={postgres_admission_count}, mongo={mongo_adm_count}) "
                f"icustays(pg={postgres_icu_stay_total}, mongo={mongo_icu_total}) "
                f"diagnoses(pg={postgres_diagnosis_total}, mongo={mongo_diag_total})"
            )

        if(failures == 0):
            print("[PASS] all nested fields for patient: subject_id={sid}, found")


    print("\n" + "-" * 40)
    print(f"Sample size: {len(sample_subject_ids)} patients")
    print(f"Nesting failures: {failures}")
    print("Nesting validation:", "PASSED" if failures == 0 else "FAILED")
    print("-" * 40 + "\n")



def check_required_fields(document, required_fields):

    missing = 0
    for f in required_fields:
        if f not in document:
            print(f"[FAIL] Missing required field: {f}")
            missing += 1
    return missing

def validate_patient_fields(mongo):
    print("="*40)
    print("Validating Patient Fields in MongoDB\n")

    required_fields = ["subject_id", "gender", "dob", "admissions"]
    failures = 0

    cursor = mongo["patients"].find({}, {"_id": 0}).limit(50)
    for doc in cursor:
        failures += check_required_fields(doc, required_fields)

    print("Patient field validation:", "PASSED" if failures == 0 else "FAILED")
    print("\n")
    return failures


def validate_admission_fields(mongo):
    print("="*40)
    print("Validating Admission Fields in MongoDB\n")

    required_fields = ["hadm_id", "admittime", "dischtime",
                       "diagnosis", "icustays", "diagnoses_icd"]

    failures = 0

    cursor = mongo["patients"].find({}, {"admissions": 1, "_id": 0}).limit(50)
    for doc in cursor:
        for adm in doc.get("admissions", []):
            failures += check_required_fields(adm, required_fields)

    print("Admission field validation:", "PASSED" if failures == 0 else "FAILED")
    print("\n")
    return failures


def validate_icu_fields(mongo):
    print("="*40)
    print("Validating ICU Stay Fields in MongoDB\n")

    required_fields = ["icustay_id", "intime", "outtime", "los"]

    failures = 0

    cursor = mongo["patients"].find({}, {"admissions.icustays": 1, "_id": 0}).limit(50)
    for doc in cursor:
        for adm in doc.get("admissions", []):
            for icu in adm.get("icustays", []):
                failures += check_required_fields(icu, required_fields)

    print("ICU Stay field validation:", "PASSED" if failures == 0 else "FAILED")
    print("\n")
    return failures


def validate_diagnosis_fields(mongo):
    print("="*40)
    print("Validating Diagnosis Fields in MongoDB\n")

    required_fields = ["icd9_code", "short_title", "long_title"]

    failures = 0

    cursor = mongo["patients"].find({}, {"admissions.diagnoses_icd": 1, "_id": 0}).limit(50)
    for doc in cursor:
        for adm in doc.get("admissions", []):
            for diag in adm.get("diagnoses_icd", []):
                failures += check_required_fields(diag, required_fields)

    print("Diagnosis field validation:", "PASSED" if failures == 0 else "FAILED")
    print("\n")
    return failures


def validate_note_fields(mongo):
    print("="*40)
    print("Validating Note Event Fields in MongoDB\n")

    required_fields = ["subject_id", "hadm_id", "chartdate", "category", "text"]

    failures = 0

    cursor = mongo["noteevents"].find({}, {"_id": 0}).limit(50)
    for doc in cursor:
        failures += check_required_fields(doc, required_fields)

    print("Note Event field validation:", "PASSED" if failures == 0 else "FAILED")
    print("\n")
    return failures


def validate_required_fields(mongo):
    print("="*40)
    print("Running ALL Required Field Validations\n")

    total_failures = 0
    total_failures += validate_patient_fields(mongo)
    total_failures += validate_admission_fields(mongo)
    total_failures += validate_icu_fields(mongo)
    total_failures += validate_diagnosis_fields(mongo)
    total_failures += validate_note_fields(mongo)

    print("="*40)
    print("FINAL REQUIRED FIELD RESULT:",
          "PASSED" if total_failures == 0 else f"FAILED ({total_failures} missing fields)")
    print("="*40 + "\n")


def main():
   
    postgres = connect_to_postgres()
    mongo = connect_to_mongo()

    print("\n"*2)
    validate_counts(postgres, mongo)
    print("\n"*2)
    validate_nesting(postgres, mongo)
    print("\n"*2)
    validate_required_fields(mongo) 


if __name__ == "__main__":
    main()
