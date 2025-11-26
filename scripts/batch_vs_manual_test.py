#!/usr/bin/env python3
"""
SOEN363 Phase 2 - Performance Testing Script (Batch vs No-Batch Inserts)

This script compares the performance of:
- batch inserts (insert_many)
- non-batch inserts (insert_one in a loop)

USAGE:
    python batch_vs_manual_test.py batch 1
    python batch_vs_manual_test.py no-batch 2

This will output either:
    reports/performance_test_results/performance_test_1_batch.csv
    reports/performance_test_results/performance_test_2_no-batch.csv

MongoDB connection assumes a running instance with credentials.
"""

import os
import sys
import csv
import time
import random
from datetime import datetime
from pymongo import MongoClient


# ================================
# CONFIG
# ================================
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:admin@localhost:27018/?authSource=admin"
)

MONGO_DB_NAME = os.getenv("MONGO_DB", "hospital_db_nosql_test")
TEST_COLLECTION = "perf_test_collection"

REPORTS_DIR = "reports/performance_test_results"


# ================================
# Helpers
# ================================

def ensure_reports_dir():
    os.makedirs(REPORTS_DIR, exist_ok=True)


def get_output_csv(test_number: str, mode: str) -> str:
    safe_mode = "batch" if mode == "batch" else "no-batch"
    return f"{REPORTS_DIR}/performance_test_{test_number}_{safe_mode}.csv"


def generate_fake_rows(n: int) -> list:
    """Generate N small fake MongoDB documents."""
    return [
        {
            "patient_id": random.randint(1, 10_000_000),
            "value": random.random(),
            "flag": bool(random.getrandbits(1)),
            "timestamp": datetime.utcnow().isoformat()
        }
        for _ in range(n)
    ]


# ================================
# Test Variants
# ================================

def test_no_batch_single_inserts(collection, num_rows: int):
    """Insert N rows using insert_one repeatedly."""
    docs = generate_fake_rows(num_rows)
    start = time.perf_counter()

    for doc in docs:
        collection.insert_one(doc)

    end = time.perf_counter()
    return end - start


def test_batch_insert_many(collection, num_rows: int):
    """Insert N rows using insert_many."""
    docs = generate_fake_rows(num_rows)
    start = time.perf_counter()

    collection.insert_many(docs)

    end = time.perf_counter()
    return end - start


# ================================
# Main test runner
# ================================

def run_tests(mode: str):
    """
    mode = "batch" or "no-batch"
    Runs 5 test sizes:
        1k, 5k, 10k, 20k, 50k
    """
    row_sizes = [1_000, 5_000, 10_000, 20_000, 50_000]

    print(f"[RUNNING] Mode: {mode}")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    collection = db[TEST_COLLECTION]

    # wipe existing test data
    collection.delete_many({})

    results = []

    for rows in row_sizes:
        print(f"  → Running test with {rows} rows...")

        if mode == "batch":
            duration = test_batch_insert_many(collection, rows)
            label = f"batch_insert_{rows}"
        else:
            duration = test_no_batch_single_inserts(collection, rows)
            label = f"no_batch_insert_{rows}"

        results.append({
            "test_name": label,
            "rows_inserted": rows,
            "duration_seconds": round(duration, 6),
            "insert_type": mode,
            "timestamp": datetime.utcnow().isoformat()
        })

        # clear collection before next run
        collection.delete_many({})

    return results


# ================================
# CSV writer
# ================================

def write_csv(path: str, rows: list):
    fieldnames = ["test_name", "rows_inserted", "duration_seconds", "insert_type", "timestamp"]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"[DONE] Results saved to: {path}")


# ================================
# Entry point
# ================================

def main():
    if len(sys.argv) != 3:
        print("Usage: python batch_vs_manual_test.py [batch|no-batch] [test_number]")
        sys.exit(1)

    mode = sys.argv[1].strip().lower()
    test_number = sys.argv[2].strip()

    if mode not in ("batch", "no-batch"):
        print("ERROR: First param must be 'batch' or 'no-batch'")
        sys.exit(1)

    ensure_reports_dir()
    output_csv = get_output_csv(test_number, mode)

    results = run_tests(mode)
    write_csv(output_csv, results)


if __name__ == "__main__":
    main()
