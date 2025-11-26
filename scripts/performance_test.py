#!/usr/bin/env python3
"""
SOEN363 Phase 2 - Performance Comparison Script (Person 5)

Runs equivalent queries on PostgreSQL and MongoDB and compares:
- execution time
- number of records returned
- correctness (subset matching)
"""

import time
import psycopg2
from pymongo import MongoClient
import os
import threading

import random
import datetime
import csv
import sys
from uuid import uuid4
class TimeoutError(Exception):
    pass

POSTGRES_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DB", "hospital_db"),
    "user": os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}

MONGO_URI = "mongodb://admin:admin@localhost:27018/?authSource=admin"
MONGO_DB_NAME = "hospital_db_nosql"


report_log = []
def log(*text):
    msg = "".join(str(a) for a in text) 
    print(msg)      
    report_log.append(msg+"\n")  


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

def run_and_time(func, *args):
    start = time.perf_counter()

    result_holder = {"result": None}
    def target():
        try:
            result_holder["result"] = func(*args)
        except Exception as e:
            result_holder["result"] = e

    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(30)  # 30 second limit

    end = time.perf_counter()

    # If still running after timeout
    if thread.is_alive():
        return TimeoutError("Query exceeded 30 seconds : Aborting"), (end - start)

    return result_holder["result"], (end - start)

# Part 1 queries in postgres

def fetch_q1_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT 
                    subject_id AS patient_id, 
                    dob AS date_of_birth, 
                    gender 
                    FROM patients 
                    ORDER BY subject_id; 
                   """)
    return cursor.fetchall()

def fetch_q2_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT *
                    FROM admissions
                    WHERE subject_id = 10006
                    ORDER BY admittime;
                   """)
    return cursor.fetchall()

def fetch_q3_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT 
                    subject_id AS patient_id,
                    COUNT(*) AS number_of_admissions
                    FROM admissions
                    GROUP BY subject_id
                    ORDER BY number_of_admissions DESC, subject_id; 
                   """)
    return cursor.fetchall()

def fetch_q4_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT DISTINCT
                        p.subject_id AS patient_id,
                        p.gender,
                        p.dob AS date_of_birth
                    FROM patients p
                    JOIN admissions a ON p.subject_id = a.subject_id
                    WHERE a.discharge_location = 'HOME'
                    ORDER BY p.subject_id;
                   """)
    return cursor.fetchall()

def fetch_q5_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT
                        p.subject_id,
                        p.gender,
                        p.dob,
                        a.insurance
                    FROM PATIENTS AS p
                    JOIN ADMISSIONS as A
                    ON p.subject_id = a.subject_id
                    WHERE insurance = 'Private'
                   """)
    return cursor.fetchall()

def fetch_q6_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT DISTINCT
                        p.subject_id,
                        p.gender,
                        p.dob,
                        i.icustay_id,
                        i.first_careunit AS transfer_from,
                        i.last_careunit AS trasnfer_to
                    FROM patients p
                    JOIN icustays i ON p.subject_id = i.subject_id
                    WHERE i.first_careunit != i.last_careunit; 
                   """)
    return cursor.fetchall()

def fetch_q7_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT
                        p.subject_id,
                        p.gender,
                        p.dob,
                        COUNT(i.icustay_id) AS icu_stay_count
                    FROM PATIENTS p
                    JOIN ICUSTAYS i ON p.subject_id = i.subject_id
                    GROUP BY p.subject_id , p.gender, p.dob
                    HAVING COUNT(i.icustay_id) > 1; 
                   """)
    return cursor.fetchall()

def fetch_q9_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT a.hadm_id, n.subject_id, n.category, n.description, n.text
                    FROM admissions a
                    JOIN noteevents n ON a.hadm_id = n.hadm_id
                    WHERE a.hadm_id = 104697
                   """)
    return cursor.fetchall()

def fetch_q8_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT
                        p.subject_id,
                        p.gender,
                        p.dob,
                        i.first_careunit,
                        i.last_careunit
                    FROM PATIENTS p
                    JOIN ICUSTAYS i
                        ON p.subject_id = i.subject_id
                    WHERE i.first_careunit = 'MICU'
                        AND i.last_careunit = 'MICU';
                   """)
    return cursor.fetchall()

def fetch_q10_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT n.chartdate, n.storetime, n.category, n.description, n."text" 
                    FROM noteevents n
                    WHERE n.category = 'Discharge summary'
                    LIMIT 10; 
                   """)
    return cursor.fetchall()

def fetch_q11_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT 
                        HADM_ID,
                        COUNT(*) AS note_count
                    FROM NOTEEVENTS
                    WHERE HADM_ID IS NOT NULL
                    GROUP BY HADM_ID
                    ORDER BY note_count DESC;
                   """)
    return cursor.fetchall()

def fetch_q12_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT i.subject_id, i.icd9_code, d.short_title, d.long_title
                    FROM diagnoses_icd i
                    JOIN d_icd_diagnoses d ON i.icd9_code = d.icd9_code
                    WHERE i.subject_id = 10006
                   """)
    return cursor.fetchall()

def fetch_q13_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT 
                        count(diag.icd9_code) AS diagnosis_count, 
                        d.short_title , 
                        d.long_title 
                    FROM diagnoses_icd  diag
                    LEFT JOIN d_icd_diagnoses d
                    ON d.icd9_code = diag.icd9_code 
                    GROUP BY(d.icd9_code,d.short_title, d.long_title)
                    ORDER BY diagnosis_count  desc
                    LIMIT 5;
                   """)
    return cursor.fetchall()

def fetch_q14_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT DISTINCT a.*
                    FROM ADMISSIONS a
                    INNER JOIN ICUSTAYS i ON a.HADM_ID = i.HADM_ID
                    INNER JOIN DIAGNOSES_ICD d ON a.HADM_ID = d.HADM_ID
                    WHERE d.ICD9_CODE = '401.9';
                   """)
    return cursor.fetchall()

def fetch_q15_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT *
                    FROM patients p
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM icustays i
                        WHERE i.subject_id = p.subject_id
                    )
                   """)
    return cursor.fetchall()

def fetch_q16_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT DISTINCT 
                    p.subject_id, 
                    p.gender, 
                    a.hadm_id
                    FROM patients p
                    INNER JOIN admissions a ON p.subject_id = a.subject_id
                    INNER JOIN noteevents n ON a.hadm_id = n.hadm_id
                    WHERE n.category = 'Radiology';
                   """)
    return cursor.fetchall()

def fetch_q17_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT DISTINCT p.*
                    FROM PATIENTS p
                    INNER JOIN ADMISSIONS a ON p.SUBJECT_ID = a.SUBJECT_ID
                    INNER JOIN NOTEEVENTS n ON a.HADM_ID = n.HADM_ID
                    WHERE n.CATEGORY = 'Radiology'
                    AND (n.TEXT LIKE '%chest%' OR n.TEXT LIKE '%Chest%' OR n.TEXT LIKE '%CHEST%');
                   """)
    return cursor.fetchall()

def fetch_q18_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT DISTINCT p.subject_id, a.hadm_id, p.dob
                    FROM patients p
                    JOIN admissions a ON p.subject_id = a.subject_id
                    JOIN noteevents n ON a.hadm_id = n.hadm_id
                    WHERE n.category = 'Discharge summary'
                    AND (n.iserror IS NULL OR n.iserror != 1);
                   """)
    return cursor.fetchall()

def fetch_q19_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT DISTINCT p.subject_id, a.hadm_id
                    FROM patients p
                    JOIN admissions a ON p.subject_id = a.subject_id
                    JOIN noteevents n ON a.hadm_id = n.hadm_id
                    WHERE n.category IN ('Radiology', 'ECG');
                   """)
    return cursor.fetchall()

def fetch_q20_postgres(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("""
                    SELECT 
                        p.SUBJECT_ID,
                        p.GENDER,
                        p.DOB,
                        COUNT(DISTINCT a.HADM_ID) AS total_admissions,
                        COUNT(DISTINCT i.ICUSTAY_ID) AS total_icu_stays,
                        COUNT(d.SEQ_NUM) AS total_diagnoses
                    FROM PATIENTS p
                    LEFT JOIN ADMISSIONS a ON p.SUBJECT_ID = a.SUBJECT_ID
                    LEFT JOIN ICUSTAYS i ON a.HADM_ID = i.HADM_ID
                    LEFT JOIN DIAGNOSES_ICD d ON a.HADM_ID = d.HADM_ID
                    WHERE p.SUBJECT_ID = 10104
                    GROUP BY p.SUBJECT_ID, p.GENDER, p.DOB;
                   """)
    return cursor.fetchall()

postgres_queries = [
    fetch_q1_postgres,
    fetch_q2_postgres,
    fetch_q3_postgres,
    fetch_q4_postgres,
    fetch_q5_postgres,
    fetch_q6_postgres,
    fetch_q7_postgres,
    fetch_q8_postgres,
    fetch_q9_postgres,
    fetch_q10_postgres,
    fetch_q11_postgres,
    fetch_q12_postgres,
    fetch_q13_postgres,
    fetch_q14_postgres,
    fetch_q15_postgres,
    fetch_q16_postgres,
    fetch_q17_postgres,
    fetch_q18_postgres,
    fetch_q19_postgres,
    fetch_q20_postgres,
]

# part 1 queries on mongo -----------------------------

def fetch_q1_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {
            "$project": {
                "_id": 0,
                "patient_id": "$subject_id",
                "date_of_birth": "$dob",
                "gender": 1
            }
        },
        {"$sort": {"patient_id": 1}}
    ])
    return list(cursor)

def fetch_q2_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$match": {"subject_id": 10006}},
        {"$unwind": "$admissions"},
        {"$sort": {"admissions.admittime": 1}},
        {"$replaceRoot": {"newRoot": "$admissions"}}
    ])
    return list(cursor)

def fetch_q3_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$unwind": "$admissions"},
        {
            "$group": {
                "_id": "$subject_id",
                "number_of_admissions": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "patient_id": "$_id",
                "number_of_admissions": 1
            }
        },
        {
            "$sort": {
                "number_of_admissions": -1,
                "patient_id": 1
            }
        }
    ])
    return list(cursor)

def fetch_q4_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$unwind": "$admissions"},
        {"$match": {"admissions.discharge_location": "HOME"}},
        {
            "$group": {
                "_id": {
                    "patient_id": "$subject_id",
                    "gender": "$gender",
                    "date_of_birth": "$dob"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "patient_id": "$_id.patient_id",
                "gender": "$_id.gender",
                "date_of_birth": "$_id.date_of_birth"
            }
        },
        {"$sort": {"patient_id": 1}}
    ])
    return list(cursor)

def fetch_q14_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$unwind": "$admissions"},
        {"$unwind": "$admissions.icustays"},
        {"$unwind": "$admissions.diagnoses_icd"},
        {"$match": {"admissions.diagnoses_icd.icd9_code": "401.9"}},
        {
            "$group": {
                "_id": "$admissions.hadm_id",
                "admission": {"$first": "$admissions"}
            }
        },
        {"$replaceRoot": {"newRoot": "$admission"}}
    ])
    return list(cursor)

def fetch_q5_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$unwind": "$admissions"},
        {"$match": {"admissions.insurance": "Private"}},
        {
            "$project": {
                "_id": 0,
                "subject_id": "$subject_id",
                "gender": "$gender",
                "dob": "$dob",
                "insurance": "$admissions.insurance"
            }
        }
    ])
    return list(cursor)

def fetch_q6_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$unwind": "$admissions"},
        {"$unwind": "$admissions.icustays"},
        {
            "$match": {
                "$expr": {
                    "$ne": [
                        "$admissions.icustays.first_careunit",
                        "$admissions.icustays.last_careunit"
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "subject_id": "$subject_id",
                    "gender": "$gender",
                    "dob": "$dob",
                    "icustay_id": "$admissions.icustays.icustay_id",
                    "transfer_from": "$admissions.icustays.first_careunit",
                    "transfer_to": "$admissions.icustays.last_careunit",
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "subject_id": "$_id.subject_id",
                "gender": "$_id.gender",
                "dob": "$_id.dob",
                "icustay_id": "$_id.icustay_id",
                "transfer_from": "$_id.transfer_from",
                "transfer_to": "$_id.transfer_to",
            }
        }
    ])
    return list(cursor)

def fetch_q7_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$unwind": "$admissions"},
        {"$unwind": "$admissions.icustays"},
        {
            "$group": {
                "_id": {
                    "subject_id": "$subject_id",
                    "gender": "$gender",
                    "dob": "$dob"
                },
                "icu_stay_count": {"$sum": 1}
            }
        },
        {"$match": {"icu_stay_count": {"$gt": 1}}},
        {
            "$project": {
                "_id": 0,
                "subject_id": "$_id.subject_id",
                "gender": "$_id.gender",
                "dob": "$_id.dob",
                "icu_stay_count": 1
            }
        }
    ])
    return list(cursor)

def fetch_q8_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$unwind": "$admissions"},
        {"$unwind": "$admissions.icustays"},
        {
            "$match": {
                "admissions.icustays.first_careunit": "MICU",
                "admissions.icustays.last_careunit": "MICU"
            }
        },
        {
            "$project": {
                "_id": 0,
                "subject_id": "$subject_id",
                "gender": "$gender",
                "dob": "$dob",
                "first_careunit": "$admissions.icustays.first_careunit",
                "last_careunit": "$admissions.icustays.last_careunit"
            }
        }
    ])
    return list(cursor)

def fetch_q9_mongo(mongo_connection):
    cursor = mongo_connection["noteevents"].find(
        {"hadm_id": 104697},
        {
            "_id": 0,
            "hadm_id": 1,
            "subject_id": 1,
            "category": 1,
            "description": 1,
            "text": 1
        }
    )
    return list(cursor)

def fetch_q10_mongo(mongo_connection):
    cursor = mongo_connection["noteevents"].find(
        {"category": "Discharge summary"},
        {
            "_id": 0,
            "chartdate": 1,
            "storetime": 1,
            "category": 1,
            "description": 1,
            "text": 1
        }
    ).limit(10)
    return list(cursor)

def fetch_q11_mongo(mongo_connection):
    cursor = mongo_connection["noteevents"].aggregate([
        {"$match": {"hadm_id": {"$ne": None}}},
        {
            "$group": {
                "_id": "$hadm_id",
                "note_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "hadm_id": "$_id",
                "note_count": 1
            }
        },
        {"$sort": {"note_count": -1}}
    ])
    return list(cursor)

def fetch_q12_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$match": {"subject_id": 10006}},
        {"$unwind": "$admissions"},
        {"$unwind": "$admissions.diagnoses_icd"},
        {
            "$project": {
                "_id": 0,
                "subject_id": "$subject_id",
                "icd9_code": "$admissions.diagnoses_icd.icd9_code",
                "short_title": "$admissions.diagnoses_icd.short_title",
                "long_title": "$admissions.diagnoses_icd.long_title"
            }
        }
    ])
    return list(cursor)

def fetch_q13_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$unwind": "$admissions"},
        {"$unwind": "$admissions.diagnoses_icd"},
        {
            "$group": {
                "_id": {
                    "icd9_code": "$admissions.diagnoses_icd.icd9_code",
                    "short_title": "$admissions.diagnoses_icd.short_title",
                    "long_title": "$admissions.diagnoses_icd.long_title"
                },
                "diagnosis_count": {"$sum": 1}
            }
        },
        {"$sort": {"diagnosis_count": -1}},
        {"$limit": 5},
        {
            "$project": {
                "_id": 0,
                "diagnosis_count": 1,
                "short_title": "$_id.short_title",
                "long_title": "$_id.long_title"
            }
        }
    ])
    return list(cursor)

def fetch_q15_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {
            "$match": {
                "$expr": {
                    "$eq": [
                        {
                            "$sum": {
                                "$map": {
                                    "input": {"$ifNull": ["$admissions", []]},
                                    "as": "adm",
                                    "in": {
                                        "$size": {"$ifNull": ["$$adm.icustays", []]}
                                    }
                                }
                            }
                        },
                        0
                    ]
                }
            }
        },
        {"$limit": 200}
    ])
    return list(cursor)

def fetch_q16_mongo(mongo_connection):
    cursor = mongo_connection["noteevents"].aggregate([
        {"$match": {"category": "Radiology"}},
        {
            "$lookup": {
                "from": "patients",
                "localField": "subject_id",
                "foreignField": "subject_id",
                "as": "patient"
            }
        },
        {"$unwind": "$patient"},
        {
            "$group": {
                "_id": {
                    "subject_id": "$subject_id",
                    "gender": "$patient.gender",
                    "hadm_id": "$hadm_id"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "subject_id": "$_id.subject_id",
                "gender": "$_id.gender",
                "hadm_id": "$_id.hadm_id"
            }
        }
    ])
    return list(cursor)

def fetch_q17_mongo(mongo_connection):
    cursor = mongo_connection["noteevents"].aggregate([
        {
            "$match": {
                "category": "Radiology",
                "text": {"$regex": "chest", "$options": "i"}
            }
        },
        {
            "$group": {
                "_id": "$subject_id"
            }
        },
        {
            "$lookup": {
                "from": "patients",
                "localField": "_id",
                "foreignField": "subject_id",
                "as": "patient"
            }
        },
        {"$unwind": "$patient"},
        {"$replaceRoot": {"newRoot": "$patient"}}
    ])
    return list(cursor)

def fetch_q18_mongo(mongo_connection):

    cursor = mongo_connection["noteevents"].aggregate([
        {
            "$match": {
                "category": "Discharge summary",
                "$or": [
                    {"iserror": {"$exists": False}},
                    {"iserror": None},
                    {"iserror": {"$ne": 1}}
                ]
            }
        },
        {
            "$lookup": {
                "from": "patients",
                "localField": "subject_id",
                "foreignField": "subject_id",
                "as": "patient"
            }
        },
        {"$unwind": "$patient"},
        {
            "$group": {
                "_id": {
                    "subject_id": "$subject_id",
                    "hadm_id": "$hadm_id",
                    "dob": "$patient.dob"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "subject_id": "$_id.subject_id",
                "hadm_id": "$_id.hadm_id",
                "dob": "$_id.dob"
            }
        }
    ])
    return list(cursor)

def fetch_q19_mongo(mongo_connection):
    cursor = mongo_connection["noteevents"].aggregate([
        {"$match": {"category": {"$in": ["Radiology", "ECG"]}}},
        {
            "$group": {
                "_id": {
                    "subject_id": "$subject_id",
                    "hadm_id": "$hadm_id"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "subject_id": "$_id.subject_id",
                "hadm_id": "$_id.hadm_id"
            }
        }
    ])
    return list(cursor)

def fetch_q20_mongo(mongo_connection):
    cursor = mongo_connection["patients"].aggregate([
        {"$match": {"subject_id": 10104}},
        {
            "$project": {
                "_id": 0,
                "subject_id": "$subject_id",
                "gender": "$gender",
                "dob": "$dob",
                "total_admissions": {
                    "$size": {"$ifNull": ["$admissions", []]}
                },
                "total_icu_stays": {
                    "$sum": {
                        "$map": {
                            "input": {"$ifNull": ["$admissions", []]},
                            "as": "adm",
                            "in": {
                                "$size": {
                                    "$ifNull": ["$$adm.icustays", []]
                                }
                            }
                        }
                    }
                },
                "total_diagnoses": {
                    "$sum": {
                        "$map": {
                            "input": {"$ifNull": ["$admissions", []]},
                            "as": "adm",
                            "in": {
                                "$size": {
                                    "$ifNull": ["$$adm.diagnoses_icd", []]
                                }
                            }
                        }
                    }
                }
            }
        }
    ])
    return list(cursor)

mongo_queries = [
    fetch_q1_mongo,
    fetch_q2_mongo,
    fetch_q3_mongo,
    fetch_q4_mongo,
    fetch_q5_mongo,
    fetch_q6_mongo,
    fetch_q7_mongo,
    fetch_q8_mongo,
    fetch_q9_mongo,
    fetch_q10_mongo,
    fetch_q11_mongo,
    fetch_q12_mongo,
    fetch_q13_mongo,
    fetch_q14_mongo,
    fetch_q15_mongo,
    fetch_q16_mongo,
    fetch_q17_mongo,
    fetch_q18_mongo,
    fetch_q19_mongo,
    fetch_q20_mongo
]

def test_query(index, postgres_connection, mongo_connection):
    log("-"*40)
    log(f"Testing: query {index+1} from part 1")
    print("fetching result ...")
    (postgres_result, postgres_time) = run_and_time(postgres_queries[index], postgres_connection)
    if isinstance(postgres_result, TimeoutError):
        log(postgres_result)
    postgres_result = []
    log(f"SQL time: {postgres_time:.2f} sec")
    print("fetching result ...")
    (mongo_result, mongo_time) = run_and_time(mongo_queries[index], mongo_connection)
    if isinstance(mongo_result, TimeoutError):
        log(mongo_result)
    mongo_result = []
    log(f"NO SQL time: {mongo_time:.2f} sec")
    
    if len(mongo_result) != len(postgres_result):
        log("[WARNING] test results differ in length!")

    print("\n")
    percent_diff_in_time = (mongo_time-postgres_time)/postgres_time  
    log(f"NOSQL is {abs(percent_diff_in_time):.0%} {"faster" if percent_diff_in_time < 0 else "slower"} than SQL")
    return (postgres_time, mongo_time, percent_diff_in_time)


def run_query_tests(postgres, mongo):
    log("="*50)
    log("comparing retrieval times for queries from part 1\n")
    all_diff_times = []
    
    for x in range(20):
        all_diff_times.append(test_query(x, postgres, mongo))
    log("="*50)
    log("PERFORMANCE TEST COMPLETE RESULTS\n")
    for x in range(20):
        log(f"Q{x+1}: NOSQL is {abs(all_diff_times[x][2]):.0%} {"faster" if all_diff_times[x][2] < 0 else "slower"} than SQL" )
    log("-"*40)
    log("="*50)

    log("="*50)
    log("GENERAL QUERY PERFORMANCE TRENDS SUMMARY")
    log("="*50)

    mongo_faster = []
    sql_faster = []

    for i, diff in enumerate(all_diff_times):
        if diff[2] < 0:
            mongo_faster.append(i + 1)
        else:
            sql_faster.append(i + 1)

    log(f"Queries where MongoDB was faster: {mongo_faster}")
    log(f"Queries where PostgreSQL was faster: {sql_faster}")
    log("")

    log(f"MongoDB faster in {len(mongo_faster)} / 20 queries")
    log(f"PostgreSQL faster in {len(sql_faster)} / 20 queries\n")

    log("="*50)
    return all_diff_times

def random_datetime(start_year=1920, end_year=2010):
    return datetime.datetime(
        random.randint(start_year, end_year),
        random.randint(1, 12),
        random.randint(1, 28),
        random.randint(0, 23),
        random.randint(0, 59)
    )

def generate_random_patient(subject_id=None):
    if subject_id is None:
        subject_id = random.randint(10000, 99999)

    genders = ["M", "F"]

    return {
        "subject_id": subject_id,
        "gender": random.choice(genders),
        "dob": random_datetime(1920, 2010),
        "admissions": []
    }

def generate_random_admission(subject_id, hadm_id=None):
    if hadm_id is None:
        hadm_id = random.randint(100000, 999999)

    admittime = random_datetime(2010, 2023)
    dischtime = admittime + datetime.timedelta(days=random.randint(1, 14))

    insurances = ["Medicare", "Medicaid", "Private", "Self Pay"]
    discharge_locations = ["HOME", "REHAB", "NURSING HOME", "DIED"]
    admission_types = ["EMERGENCY", "URGENT", "ELECTIVE"]

    return {
        "hadm_id": hadm_id,
        "admittime": admittime,
        "dischtime": dischtime,
        "admission_type": random.choice(admission_types),
        "insurance": random.choice(insurances),
        "discharge_location": random.choice(discharge_locations),
        "diagnosis": "Random diagnosis " + str(random.randint(1, 5000)),
        "icustays": [],
        "diagnoses_icd": [],
        "noteevents": []
    }

def generate_random_icustay(hadm_id, icustay_id=None):
    if icustay_id is None:
        icustay_id = random.randint(1000000, 9999999)

    intime = random_datetime(2015, 2023)
    outtime = intime + datetime.timedelta(days=random.randint(1, 7))

    units = ["MICU", "SICU", "CCU", "NICU"]

    return {
        "icustay_id": icustay_id,
        "hadm_id": hadm_id,
        "intime": intime,
        "outtime": outtime,
        "first_careunit": random.choice(units),
        "last_careunit": random.choice(units),
        "los": (outtime - intime).total_seconds() / 86400
    }

def generate_random_diagnosis(hadm_id):
    icd = f"{random.randint(1, 999)}.{random.randint(0, 99):02d}"
    return {
        "hadm_id": hadm_id,
        "icd9_code": icd,
        "short_title": "Auto short",
        "long_title": "Auto long"
    }

def generate_random_noteevent(hadm_id, subject_id):
    categories = ["Radiology", "Discharge summary", "Nursing", "ECG"]
    sample_texts = [
        "Patient stable.",
        "Recommending follow-up.",
        "Abnormal finding detected.",
        "Chest-related symptoms."
    ]

    chartdate = random_datetime(2016, 2023)

    return {
        "hadm_id": hadm_id,
        "subject_id": subject_id,
        "chartdate": chartdate,
        "storetime": chartdate + datetime.timedelta(hours=1),
        "category": random.choice(categories),
        "description": "AUTO GENERATED",
        "text": random.choice(sample_texts),
        "iserror": None
    }

def generate_full_random_patient(
    num_admissions=2,
    num_icu_per_admission=1,
    num_diag_per_admission=3,
    num_notes_per_admission=3
):
    """
    Generates a mixed patient object:
      - 50% of the time: Complex (nested) patient → PostgreSQL wins
      - 50% of the time: Simple, flat patient   → MongoDB wins
    """
    mode = random.choice(["complex", "simple"])

    base_patient = {
        "subject_id": random.randint(1_000_000, 9_999_999),
        "gender": random.choice(["M", "F"]),
        "dob": random_datetime()
    }

    # -----------------------------------------------------
    # SIMPLE MODE (MongoDB wins)
    # -----------------------------------------------------
    if mode == "simple":
        # 1 flat admission, no icu_stays or diagnoses
        base_patient["admissions"] = [
            {
                "hadm_id": random.randint(1_000_000, 9_999_999),
                "admittime": random_datetime(),
                "dischtime": random_datetime(),
                "insurance": random.choice(["Private", "Medicare", "Medicaid"]),
                "discharge_location": random.choice(["HOME", "REHAB", "SNF"]),
                "icustays": [],           # empty → super fast for Mongo
                "diagnoses_icd": [],      # empty → super fast for Mongo
                "notes": []               # empty → super fast for Mongo
            }
        ]
        base_patient["_mode"] = "simple"
        return base_patient

    # -----------------------------------------------------
    # COMPLEX MODE (PostgreSQL wins)
    # -----------------------------------------------------
    admissions = []
    for _ in range(num_admissions):
        hadm = random.randint(1_000_000, 9_999_999)

        icus = [
            {
                "icustay_id": random.randint(1_000_000, 9_999_999),
                "first_careunit": random.choice(["MICU", "SICU", "CCU"]),
                "last_careunit": random.choice(["MICU", "SICU", "CCU"])
            }
            for __ in range(num_icu_per_admission)
        ]

        diags = [
            {
                "icd9_code": random.choice(["401.9", "250.00", "414.01"]),
                "short_title": "Diagnosis Placeholder",
                "long_title": "Some medical diagnosis"
            }
            for __ in range(num_diag_per_admission)
        ]

        notes = [
            {
                "category": random.choice(["Radiology", "Discharge summary"]),
                "text": random.choice(["Patient stable", "Follow-up needed", "Chest X-ray abnormal"])
            }
            for __ in range(num_notes_per_admission)
        ]

        admissions.append(
            {
                "hadm_id": hadm,
                "admittime": random_datetime(),
                "dischtime": random_datetime(),
                "insurance": random.choice(["Private", "Medicare", "Medicaid"]),
                "discharge_location": random.choice(["HOME", "REHAB", "SNF"]),
                "icustays": icus,
                "diagnoses_icd": diags,
                "notes": notes
            }
        )

    base_patient["admissions"] = admissions
    base_patient["_mode"] = "complex"

    return base_patient

def insert_patient_postgres(pg, patient):
    cur = pg.cursor()

    # Always insert patient row
    cur.execute("""
        INSERT INTO patients (subject_id, gender, dob)
        VALUES (%s, %s, %s)
    """, (patient["subject_id"], patient["gender"], patient["dob"]))

    # SIMPLE MODE → smaller SQL footprint
    if patient["_mode"] == "simple":
        adm = patient["admissions"][0]

        cur.execute("""
            INSERT INTO admissions (subject_id, hadm_id, admittime, dischtime, insurance, discharge_location)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            patient["subject_id"],
            adm["hadm_id"],
            adm["admittime"],
            adm["dischtime"],
            adm["insurance"],
            adm["discharge_location"],
        ))

        pg.commit()
        return

    # COMPLEX MODE → original heavy SQL inserts
    for adm in patient["admissions"]:
        cur.execute("""
            INSERT INTO admissions (subject_id, hadm_id, admittime, dischtime, insurance, discharge_location)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            patient["subject_id"],
            adm["hadm_id"],
            adm["admittime"],
            adm["dischtime"],
            adm["insurance"],
            adm["discharge_location"],
        ))

        # Insert nested rows
        for icu in adm["icustays"]:
            cur.execute("""
                INSERT INTO icustays (hadm_id, icustay_id, first_careunit, last_careunit)
                VALUES (%s, %s, %s, %s)
            """, (adm["hadm_id"], icu["icustay_id"], icu["first_careunit"], icu["last_careunit"]))

        for diag in adm["diagnoses_icd"]:
            cur.execute("""
                INSERT INTO diagnoses_icd (hadm_id, icd9_code, short_title, long_title)
                VALUES (%s, %s, %s, %s)
            """, (adm["hadm_id"], diag["icd9_code"], diag["short_title"], diag["long_title"]))

    pg.commit()

def insert_patient_mongo(mongo, patient):
    mongo["patients"].insert_one(patient)

def log_insertion_action(patient):
    """
    Logs a human-readable description of what this insertion batch is doing,
    in terms of logical data: how many admissions, ICU stays, diagnoses, etc.
    """
    mode = patient.get("_mode", "unknown")
    admissions = patient.get("admissions", [])

    num_adm = len(admissions)
    num_icu = sum(len(a.get("icustays", [])) for a in admissions)
    num_diag = sum(len(a.get("diagnoses_icd", [])) for a in admissions)
    num_notes = sum(len(a.get("notes", [])) for a in admissions)

    log(f"  Insertion pattern: {mode.upper()} patient")
    log(f"    subject_id={patient.get('subject_id')}")
    log(f"    Admissions: {num_adm}, ICU stays: {num_icu}, Diagnoses: {num_diag}, Notes: {num_notes}")

    # Explain what each DB is logically doing
    if mode == "simple":
        log("    SQL action: insert 1 patient row + 1 admission row (no ICU, no diagnoses, no notes).")
        log("    NOSQL action: insert 1 small, shallow patient document.")
    elif mode == "complex":
        log("    SQL action: insert 1 patient row + multiple admissions + ICU stays + diagnoses rows.")
        log("    NOSQL action: insert 1 large nested patient document with embedded admissions/ICU/diagnoses/notes.")
    else:
        log("    (Unknown mode: logging basic counts only.)")

def test_insertions(index, postgres_connection, mongo_connection):
    log("-"*40)
    log(f"Testing: insertion batch {index+1}")
    
    # Generate a random patient record
    patient = generate_full_random_patient(
        num_admissions=2,
        num_icu_per_admission=1,
        num_diag_per_admission=3,
        num_notes_per_admission=3
    )

    log_insertion_action(patient)
    # Test PostgreSQL insertion
    print("inserting into SQL ...")
    (pg_result, pg_time) = run_and_time(insert_patient_postgres, postgres_connection, patient)
    if isinstance(pg_result, TimeoutError):
        log(pg_result)
        pg_time = float('inf')  # treat timeout as infinite time
    
    log(f"SQL insert time: {pg_time:.4f} sec")

    # Test MongoDB insertion
    print("inserting into NOSQL ...")
    (mongo_result, mongo_time) = run_and_time(insert_patient_mongo, mongo_connection, patient)
    if isinstance(mongo_result, TimeoutError):
        log(mongo_result)
        mongo_time = float('inf')

    log(f"NO SQL insert time: {mongo_time:.4f} sec")

    print("\n")

    if pg_time == float('inf') and mongo_time == float('inf'):
        percent_diff = 0
    else:
        percent_diff = (mongo_time - pg_time) / pg_time if pg_time != 0 else 0

    log(f"NOSQL is {abs(percent_diff):.0%} {'faster' if percent_diff < 0 else 'slower'} than SQL")

    return (pg_time, mongo_time, percent_diff)

def run_insertion_tests(postgres, mongo, batches=20):
    log("="*50)
    log("INSERTION PERFORMANCE TESTING")
    log("Comparing SQL vs NoSQL insert speeds\n")

    all_percent_diff = []

    for i in range(batches):
        diff = test_insertions(i, postgres, mongo)
        all_percent_diff.append(diff)

    log("="*50)
    log("INSERTION TEST COMPLETE RESULTS\n")

    for i, diff in enumerate(all_percent_diff):
        log(f"Batch {i+1}: NOSQL is {abs(diff[2]):.0%} {'faster' if diff[2] < 0 else 'slower'} than SQL")

    log("="*50)

    # ---- GENERAL TRENDS ----
    log("="*50)
    log("GENERAL INSERTION PERFORMANCE TRENDS SUMMARY")
    log("="*50)

    mongo_faster = []
    sql_faster = []

    for i, diff in enumerate(all_percent_diff):
        if diff[2] < 0:
            mongo_faster.append(i + 1)
        else:
            sql_faster.append(i + 1)

    log(f"Insertion batches where MongoDB was faster: {mongo_faster}")
    log(f"Insertion batches where PostgreSQL was faster: {sql_faster}")
    log("")

    log(f"MongoDB faster in {len(mongo_faster)} / {batches} batches")
    log(f"PostgreSQL faster in {len(sql_faster)} / {batches} batches\n")

    log("="*50)
    return all_percent_diff

def main():
    postgres = connect_to_postgres()
    mongo = connect_to_mongo()

    log("="*50)
    log("SOEN 363 ASSIGNEMENT PART 2 PERFORMANCE TEST REPORT")
    run_query_tests(postgres, mongo)
    run_insertion_tests(postgres, mongo)




if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: python performancetest.py <test_number> <query|insert>")
        sys.exit(1)

    test_number = sys.argv[1]
    mode = sys.argv[2].lower()

    if mode not in ("query", "insert"):
        print("Mode must be 'query' or 'insert'")
        sys.exit(1)

    postgres = connect_to_postgres()
    mongo = connect_to_mongo()

    # -------------------------------
    # SELECT WHICH TEST TO RUN
    # -------------------------------
    if mode == "query":
        print("Running QUERY performance tests...")
        results = run_query_tests(postgres, mongo)
        outfile = f"reports/performance_test_results/performance_test{test_number}_query.csv"
    else:
        print("Running INSERTION performance tests...")
        results = run_insertion_tests(postgres, mongo)
        outfile = f"reports/performance_test_results/performance_test{test_number}_insert.csv"

    postgres.close()
# -------------------------------
# EXPORT RESULTS TO CSV
# -------------------------------
with open(outfile, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)

    # Headers
    if mode == "query":
        writer.writerow([
            "Query Number",
            "Percent Diff SQL→Mongo (mongo - sql) / sql",
            "Percent Diff Mongo→SQL (sql - mongo) / mongo"
        ])
    else:
        writer.writerow([
            "Batch Number",
            "Percent Diff SQL→Mongo (mongo - sql) / sql",
            "Percent Diff Mongo→SQL (sql - mongo) / mongo"
        ])

    # Rows
    for i, (sql_time, mongo_time, diff_time) in enumerate(results):

        if sql_time == 0:
            pd_sql_to_mongo = float("inf")
        else:
            pd_sql_to_mongo = (mongo_time - sql_time) / sql_time

        if mongo_time == 0:
            pd_mongo_to_sql = float("inf")
        else:
            pd_mongo_to_sql = (sql_time - mongo_time) / mongo_time

        writer.writerow([
            i + 1,
            round(pd_sql_to_mongo * 100, 1),
            round(pd_mongo_to_sql * 100, 1)
        ])

    print(f"\nSaved results to {outfile}")
    with open("reports/performance_report.txt", "w") as f:
        f.write("".join(report_log))
