#!/usr/bin/env python3
"""
SOEN363 Phase 2 - Validation Script
Validates that PostgreSQL data is correctly migrated into MongoDB.
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

report_log = []

def log(*text, sep=" ", end="\n"):
    msg = sep.join(str(a) for a in text) + end
    print(msg, end="")      
    report_log.append(msg)  

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
        log("[Pass]")
    else:
        log("[Fail] Count is not the same")
        exit(1)

def validate_patient_count(postgres_connection, mongo_connection):
    log("="*40)
    log("Verifying patient count\n")
    postgres_start_time = time.perf_counter()
    postgres_patient_count = fetch_patients_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    log(f"{postgres_patient_count} patients found in SQL DB")
    log(f"({postgres_elapsed_time}) seconds")
    log("\n")

    mongo_start_time = time.perf_counter()
    mongo_patient_count = fetch_patients_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    log(f"{mongo_patient_count} patients found in NOSQL DB")
    log(f"({mongo_elapsed_time}) seconds")
    log("\n")
    compare_count(postgres_patient_count , mongo_patient_count)

def validate_admission_count(postgres_connection, mongo_connection):
    log("="*40)
    log("Verifying admission count\n")
    postgres_start_time = time.perf_counter()
    postgres_admission_count = fetch_admission_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    log(f"{postgres_admission_count} admmissions found in SQL DB")
    log(f"({postgres_elapsed_time}) seconds")
    log("\n")

    mongo_start_time = time.perf_counter()
    mongo_admission_count = fetch_admission_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    log(f"{mongo_admission_count} admissions found in NOSQL DB")
    log(f"({mongo_elapsed_time}) seconds")
    log("\n")
    compare_count(postgres_admission_count , mongo_admission_count)

def validate_diagnosis_count(postgres_connection, mongo_connection):
    log("="*40)
    log("Verifying diagnosis count\n")
    postgres_start_time = time.perf_counter()
    postgres_diagnosis_count = fetch_diagnosis_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    log(f"{postgres_diagnosis_count} diagnoses found in SQL DB")
    log(f"({postgres_elapsed_time}) seconds")
    log("\n")

    mongo_start_time = time.perf_counter()
    mongo_diagnosis_count = fetch_diagnosis_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    log(f"{mongo_diagnosis_count} diagnoses found in NOSQL DB")
    log(f"({mongo_elapsed_time}) seconds")
    log("\n")
    compare_count(postgres_diagnosis_count , mongo_diagnosis_count)

def validate_icu_stay_count(postgres_connection, mongo_connection):
    log("="*40)
    log("Verifying icu stay count\n")
    postgres_start_time = time.perf_counter()
    postgres_icu_stay_count = fetch_icu_stay_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    log(f"{postgres_icu_stay_count} icu stays found in SQL DB")
    log(f"({postgres_elapsed_time}) seconds")
    log("\n")

    mongo_start_time = time.perf_counter()
    mongo_icu_stay_count = fetch_icu_stay_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    log(f"{mongo_icu_stay_count} icu stays found in NOSQL DB")
    log(f"({mongo_elapsed_time}) seconds")
    log("\n")
    compare_count(postgres_icu_stay_count , mongo_icu_stay_count)

def validate_note_event_count(postgres_connection, mongo_connection):
    log("="*40)
    log("Verifying note event count\n")
    postgres_start_time = time.perf_counter()
    postgres_note_event_count = fetch_note_event_count_postgres(postgres_connection)
    postgres_end_time = time.perf_counter()
    postgres_elapsed_time = postgres_end_time - postgres_start_time
    log(f"{postgres_note_event_count} note events found in SQL DB")
    log(f"({postgres_elapsed_time}) seconds")
    log("\n")

    mongo_start_time = time.perf_counter()
    mongo_note_event_count = fetch_note_event_count_mongo(mongo_connection)
    mongo_end_time = time.perf_counter()
    mongo_elapsed_time = mongo_end_time - mongo_start_time
    log(f"{mongo_note_event_count} patients found in NOSQL DB")
    log(f"({mongo_elapsed_time}) seconds")
    log("\n")
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
    log("="*40)
    log("Validating Nesting in NO SQL DB")

    sample_size = 100
    sample_postgres_patients = fetch_postgres_patients_random(postgres, sample_size)
    sample_subject_ids = [row[1] for row in sample_postgres_patients]

    if sample_subject_ids:
        log(f"Selected a random sample of {sample_size} patients in SQL DB")
        log("\n")
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
        print("Validating that all fields were properly nested in NO SQL DB...")
        print("\n")

        doc = mongo["patients"].find_one(
            {"subject_id": sid},
            {"admissions": 1, "_id": 0}
        )

        if not doc:
            failures += 1
            log(f"[FAIL] subject_id={sid}: missing patient doc in Mongo")

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
            log(
                f"[FAIL] subject_id={sid} "
                f"admissions(pg={postgres_admission_count}, mongo={mongo_adm_count}) "
                f"icustays(pg={postgres_icu_stay_total}, mongo={mongo_icu_total}) "
                f"diagnoses(pg={postgres_diagnosis_total}, mongo={mongo_diag_total})"
            )


        if(failures == 0):
            log(f"[PASS] all nested fields for patient: subject_id={sid}, found")
            log("\n", "="*40)


    log("\n" + "-" * 40)
    log(f"Sample size: {len(sample_subject_ids)} patients")
    log(f"Nesting failures: {failures}")
    log("Nesting validation:", "PASSED" if failures == 0 else "FAILED")
    log("-" * 40 + "\n")



def check_required_fields(document, required_fields):

    missing = 0
    for f in required_fields:
        if f not in document:
            log(f"[FAIL] Missing required field: {f}")
            missing += 1
    return missing

def validate_patient_fields(mongo):
    log("="*40)
    log("Validating Patient Fields in MongoDB\n")

    required_fields = ["subject_id", "gender", "dob", "admissions"]
    failures = 0

    cursor = mongo["patients"].find({}, {"_id": 0}).limit(50)
    for doc in cursor:
        failures += check_required_fields(doc, required_fields)

    log("Patient field validation:", "PASSED" if failures == 0 else "FAILED")
    log("\n")
    return failures


def validate_admission_fields(mongo):
    log("="*40)
    log("Validating Admission Fields in NOSQL DB...\n")

    required_fields = ["hadm_id", "admittime", "dischtime",
                       "diagnosis", "icustays", "diagnoses_icd"]

    failures = 0

    cursor = mongo["patients"].find({}, {"admissions": 1, "_id": 0}).limit(50)
    for doc in cursor:
        for adm in doc.get("admissions", []):
            failures += check_required_fields(adm, required_fields)

    log("Admission field validation:", "PASSED" if failures == 0 else "FAILED")
    log(f"{required_fields} found in NOSQL DB")
    log("\n")
    return failures


def validate_icu_fields(mongo):
    log("="*40)
    log("Validating ICU Stay Fields in NOSQL DB...\n")

    required_fields = ["icustay_id", "intime", "outtime", "los"]

    failures = 0

    cursor = mongo["patients"].find({}, {"admissions.icustays": 1, "_id": 0}).limit(50)
    for doc in cursor:
        for adm in doc.get("admissions", []):
            for icu in adm.get("icustays", []):
                failures += check_required_fields(icu, required_fields)

    log("ICU Stay field validation:", "PASSED" if failures == 0 else "FAILED")
    log(f"{required_fields} found in NOSQL DB")
    log("\n")
    return failures


def validate_diagnosis_fields(mongo):
    log("="*40)
    log("Validating Diagnosis Fields in NOSQL DB...\n")

    required_fields = ["icd9_code", "short_title", "long_title"]

    failures = 0

    cursor = mongo["patients"].find({}, {"admissions.diagnoses_icd": 1, "_id": 0}).limit(50)
    for doc in cursor:
        for adm in doc.get("admissions", []):
            for diag in adm.get("diagnoses_icd", []):
                failures += check_required_fields(diag, required_fields)

    log("Diagnosis field validation:", "PASSED" if failures == 0 else "FAILED")
    log(f"{required_fields} found in NOSQL DB")
    log("\n")
    return failures


def validate_note_fields(mongo):
    log("="*40)
    log("Validating Note Event Fields in NOSQL DB...\n")

    required_fields = ["subject_id", "hadm_id", "chartdate", "category", "text"]

    failures = 0

    cursor = mongo["noteevents"].find({}, {"_id": 0}).limit(50)
    for doc in cursor:
        failures += check_required_fields(doc, required_fields)

    log("Note Event field validation:", "PASSED" if failures == 0 else "FAILED")
    log(f"{required_fields} found in NOSQL DB")
    log("\n")
    return failures


def validate_required_fields(mongo):
    log("="*40)
    log("Running ALL Required Field Validations\n")

    total_failures = 0
    total_failures += validate_patient_fields(mongo)
    total_failures += validate_admission_fields(mongo)
    total_failures += validate_icu_fields(mongo)
    total_failures += validate_diagnosis_fields(mongo)
    total_failures += validate_note_fields(mongo)

    log("="*40)
    log("PASSED: All required fields found" if total_failures == 0 else f"FAILED ({total_failures} missing fields)")
    log("="*40 + "\n")

def postgres_null_count(pg, table, field):
    cur = pg.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {field} IS NULL;")
    return cur.fetchone()[0]

def mongo_null_count(mongo, collection, field):
    # count null or missing
    return mongo[collection].count_documents({
        "$or": [{field: None}, {field: {"$exists": False}}]
    })

def compare_null_counts(pg, mongo, pg_table, mongo_collection, field_label, pg_field, mongo_field):
    print(f"Checking NULL consistency for: {field_label}")

    pg_nulls = postgres_null_count(pg, pg_table, pg_field)
    mongo_nulls = mongo_null_count(mongo, mongo_collection, mongo_field)

    print(f"  Postgres NULLs = {pg_nulls}")
    print(f"  Mongo NULLs    = {mongo_nulls}")

    if pg_nulls == mongo_nulls:
        print("  [PASS] NULL counts match\n")
        return 0
    else:
        print("  [FAIL] NULL counts do not match\n")
        return 1

def validate_null_values(pg, mongo):
    print("="*40)
    print("VALIDATING NULL VALUE CONSISTENCY\n")

    failures = 0

    # Fields allowed to be nullable but SHOULD match between SQL and Mongo
    NULL_CHECKS = [
        # (pg_table, mongo_collection, label, pg_field, mongo_field)
        ("patients", "patients", "Patient Date of Death", "dod", "dod"),
        ("patients", "patients", "Patient Hospital DOD", "dod_hosp", "dod_hosp"),
        ("admissions", "patients", "Admission Deathtime", "deathtime", "admissions.deathtime"),
        ("icustays", "patients", "ICU Outtime", "outtime", "admissions.icustays.outtime"),
        ("diagnoses_icd", "patients", "Diagnosis Long Title", "icd9_code", "admissions.diagnoses_icd.long_title"),
        ("noteevents", "noteevents", "Note Chartdate", "chartdate", "chartdate"),
    ]

    for pg_table, mongo_col, label, pg_field, mongo_field in NULL_CHECKS:
        failures += compare_null_counts(
            pg, mongo,
            pg_table=pg_table,
            mongo_collection=mongo_col,
            field_label=label,
            pg_field=pg_field,
            mongo_field=mongo_field
        )

    print("-"*40)
    print("NULL VALIDATION:", "PASSED" if failures == 0 else f"FAILED ({failures} mismatches)")
    print("-"*40 + "\n")
    return failures

def postgres_missing_fk(pg, child_table, child_field, parent_table, parent_field):
    cur = pg.cursor()
    cur.execute(f"""
        SELECT COUNT(*)
        FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{child_field} = p.{parent_field}
        WHERE p.{parent_field} IS NULL;
    """)
    return cur.fetchone()[0]

def validate_integrity(pg, mongo):
    print("="*40)
    print("VALIDATING REFERENTIAL INTEGRITY\n")

    failures = 0

    # 1) Admissions → Patients
    missing = postgres_missing_fk(pg, "admissions", "subject_id", "patients", "subject_id")
    if missing > 0:
        print(f"[FAIL] {missing} admissions refer to missing patients")
        failures += 1
    else:
        print("[PASS] All admissions link to valid patients")

    # 2) ICU → Admissions
    missing = postgres_missing_fk(pg, "icustays", "hadm_id", "admissions", "hadm_id")
    if missing > 0:
        print(f"[FAIL] {missing} ICU stays refer to missing admissions")
        failures += 1
    else:
        print("[PASS] All ICU stays link to valid admissions")

    # 3) Diagnoses → Admissions
    missing = postgres_missing_fk(pg, "diagnoses_icd", "hadm_id", "admissions", "hadm_id")
    if missing > 0:
        print(f"[FAIL] {missing} diagnoses refer to missing admissions")
        failures += 1
    else:
        print("[PASS] All diagnoses link to valid admissions")

    # 4) Notes → Patients
    missing = postgres_missing_fk(pg, "noteevents", "subject_id", "patients", "subject_id")
    if missing > 0:
        print(f"[FAIL] {missing} note events refer to missing patients")
        failures += 1
    else:
        print("[PASS] All note events link to valid patients")

    # 5) Notes → Admissions
    missing = postgres_missing_fk(pg, "noteevents", "hadm_id", "admissions", "hadm_id")
    if missing > 0:
        print(f"[FAIL] {missing} note events refer to missing admissions")
        failures += 1
    else:
        print("[PASS] All note events link to valid admissions")

    print("\nINTEGRITY VALIDATION:", "PASSED" if failures == 0 else "FAILED")
    print("="*40 + "\n")

    return failures


def main():
   
    postgres = connect_to_postgres()
    mongo = connect_to_mongo()

    log("\n"*2)
    validate_counts(postgres, mongo)
    log("\n"*2)
    validate_required_fields(mongo) 
    log("\n"*2)
    validate_nesting(postgres, mongo)
    log("\n"*2)
    validate_null_values(postgres, mongo)
    log("\n"*2)
    validate_integrity(postgres, mongo)

    with open("reports/validation_report.txt", "w") as f:
        f.write("".join(report_log))
    

if __name__ == "__main__":
    main()
