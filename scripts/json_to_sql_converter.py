"""
SOEN363 Phase 2 - Convert JSON to PostgreSQL INSERT Statements
Convert scaled JSON files to SQL INSERT statements for PostgreSQL
"""

import json
import os
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def escape_sql_string(value):
    """Escape special characters in SQL strings"""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    # Escape single quotes by doubling them
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"

def load_json(json_file):
    """Load JSON file"""
    try:
        print(f"[LOAD] Reading JSON file: {json_file}")
        with open(json_file, 'r') as f:
            data = json.load(f)

        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'jsonb_agg' in data:
            return data['jsonb_agg']
        else:
            return list(data.values()) if isinstance(data, dict) else []
    except Exception as e:
        print(f"✗ Error loading JSON: {e}")
        return []

def generate_sql_inserts(patients_data, output_file):
    """Generate SQL INSERT statements from patient data"""
    print(f"\n[GENERATE] Creating SQL INSERT statements...")

    patient_row_id = 1
    admission_row_id = 1
    icustay_row_id = 1
    diagnoses_row_id = 1

    inserts = []

    # Start transactions
    inserts.append("BEGIN TRANSACTION;")
    inserts.append("SET CONSTRAINTS ALL DEFERRED;")
    inserts.append("")

    for patient in patients_data:
        subject_id = patient.get("subject_id")
        gender = escape_sql_string(patient.get("gender"))
        dob = escape_sql_string(patient.get("dob"))
        dod = escape_sql_string(patient.get("dod"))
        dod_hosp = escape_sql_string(patient.get("dod_hosp"))
        dod_ssn = escape_sql_string(patient.get("dod_ssn"))
        expire_flag = patient.get("expire_flag", 0)

        # Insert patient
        insert_stmt = f"""INSERT INTO patients (row_id, subject_id, gender, dob, dod, dod_hosp, dod_ssn, expire_flag) VALUES ({patient_row_id}, {subject_id}, {gender}, {dob}, {dod}, {dod_hosp}, {dod_ssn}, {expire_flag});"""
        inserts.append(insert_stmt)
        patient_row_id += 1

        # Insert admissions
        for admission in patient.get("admissions", []):
            hadm_id = admission.get("hadm_id")
            admittime = escape_sql_string(admission.get("admittime"))
            dischtime = escape_sql_string(admission.get("dischtime"))
            deathtime = escape_sql_string(admission.get("deathtime"))
            admission_type = escape_sql_string(admission.get("admission_type"))
            admission_location = escape_sql_string(admission.get("admission_location"))
            discharge_location = escape_sql_string(admission.get("discharge_location"))
            insurance = escape_sql_string(admission.get("insurance"))
            language = escape_sql_string(admission.get("language"))
            religion = escape_sql_string(admission.get("religion"))
            marital_status = escape_sql_string(admission.get("marital_status"))
            ethnicity = escape_sql_string(admission.get("ethnicity"))
            edregtime = escape_sql_string(admission.get("edregtime"))
            edouttime = escape_sql_string(admission.get("edouttime"))
            diagnosis = escape_sql_string(admission.get("diagnosis"))
            hospital_expire_flag = admission.get("hospital_expire_flag", 0)
            has_chartevents_data = admission.get("has_chartevents_data", 0)

            insert_stmt = f"""INSERT INTO admissions (row_id, subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admission_location, discharge_location, insurance, language, religion, marital_status, ethnicity, edregtime, edouttime, diagnosis, hospital_expire_flag, has_chartevents_data) VALUES ({admission_row_id}, {subject_id}, {hadm_id}, {admittime}, {dischtime}, {deathtime}, {admission_type}, {admission_location}, {discharge_location}, {insurance}, {language}, {religion}, {marital_status}, {ethnicity}, {edregtime}, {edouttime}, {diagnosis}, {hospital_expire_flag}, {has_chartevents_data});"""
            inserts.append(insert_stmt)
            admission_row_id += 1

            # Insert ICU stays
            for icustay in admission.get("icustays", []):
                icustay_id = icustay.get("icustay_id")
                dbsource = escape_sql_string(icustay.get("dbsource"))
                first_careunit = escape_sql_string(icustay.get("first_careunit"))
                last_careunit = escape_sql_string(icustay.get("last_careunit"))
                first_wardid = icustay.get("first_wardid")
                if first_wardid is None:
                    first_wardid = "NULL"
                last_wardid = icustay.get("last_wardid")
                if last_wardid is None:
                    last_wardid = "NULL"
                intime = escape_sql_string(icustay.get("intime"))
                outtime = escape_sql_string(icustay.get("outtime"))
                los = icustay.get("los")
                if los is None:
                    los = "NULL"

                insert_stmt = f"""INSERT INTO icustays (row_id, subject_id, hadm_id, icustay_id, dbsource, first_careunit, last_careunit, first_wardid, last_wardid, intime, outtime, los) VALUES ({icustay_row_id}, {subject_id}, {hadm_id}, {icustay_id}, {dbsource}, {first_careunit}, {last_careunit}, {first_wardid}, {last_wardid}, {intime}, {outtime}, {los});"""
                inserts.append(insert_stmt)
                icustay_row_id += 1

            # Insert diagnoses
            for diagnosis_icd in admission.get("diagnoses_icd", []):
                seq_num = diagnosis_icd.get("seq_num")
                icd9_code = escape_sql_string(diagnosis_icd.get("icd9_code"))

                insert_stmt = f"""INSERT INTO diagnoses_icd (row_id, subject_id, hadm_id, seq_num, icd9_code) VALUES ({diagnoses_row_id}, {subject_id}, {hadm_id}, {seq_num}, {icd9_code});"""
                inserts.append(insert_stmt)
                diagnoses_row_id += 1

    # Commit
    inserts.append("")
    inserts.append("COMMIT;")

    # Write to file
    with open(output_file, 'w') as f:
        f.write('\n'.join(inserts))

    print(f"✓ Generated {len(inserts)} SQL statements")
    print(f"✓ Saved to: {output_file}")
    return len(inserts)

def load_noteevents_sql(noteevents_data, output_file):
    """Generate SQL INSERT statements for noteevents"""
    print(f"\n[GENERATE] Creating noteevents SQL INSERT statements...")

    notevent_row_id = 1
    inserts = []

    inserts.append("BEGIN TRANSACTION;")
    inserts.append("SET CONSTRAINTS ALL DEFERRED;")
    inserts.append("")

    for note in noteevents_data:
        subject_id = note.get("subject_id")
        hadm_id = note.get("hadm_id")
        chartdate = escape_sql_string(note.get("chartdate"))
        charttime = escape_sql_string(note.get("charttime"))
        storetime = escape_sql_string(note.get("storetime"))
        category = escape_sql_string(note.get("category"))
        description = escape_sql_string(note.get("description"))
        cgid = note.get("cgid")
        if cgid is None:
            cgid = "NULL"
        iserror = note.get("iserror", 0)
        text = escape_sql_string(note.get("text", ""))

        insert_stmt = f"""INSERT INTO noteevents (row_id, subject_id, hadm_id, chartdate, charttime, storetime, category, description, cgid, iserror, text) VALUES ({notevent_row_id}, {subject_id}, {hadm_id}, {chartdate}, {charttime}, {storetime}, {category}, {description}, {cgid}, {iserror}, {text});"""
        inserts.append(insert_stmt)
        notevent_row_id += 1

    inserts.append("")
    inserts.append("COMMIT;")

    with open(output_file, 'w') as f:
        f.write('\n'.join(inserts))

    print(f"✓ Generated {len(inserts)} noteevents SQL statements")
    print(f"✓ Saved to: {output_file}")
    return len(inserts)

def main():
    print("\n" + "=" * 70)
    print("SOEN363 PHASE 2 - JSON TO SQL CONVERTER")
    print("Convert scaled JSON files to PostgreSQL INSERT statements")
    print("=" * 70)

    # Load patient data
    patients_json = os.path.join(PROJECT_ROOT, "scaled_JSON_output.json")
    patients_data = load_json(patients_json)

    if not patients_data:
        print("✗ Failed to load patient data")
        return

    print(f"✓ Loaded {len(patients_data):,} patients")

    # Generate SQL for patients
    patients_sql = os.path.join(PROJECT_ROOT, "sql", "insert_scaled_patients.sql")
    os.makedirs(os.path.dirname(patients_sql), exist_ok=True)
    generate_sql_inserts(patients_data, patients_sql)

    # Load noteevents data
    noteevents_json = os.path.join(PROJECT_ROOT, "scaled_JSON_output_notes.json")
    noteevents_data = load_json(noteevents_json)

    if noteevents_data:
        print(f"✓ Loaded {len(noteevents_data):,} note events")
        noteevents_sql = os.path.join(PROJECT_ROOT, "sql", "insert_scaled_noteevents.sql")
        load_noteevents_sql(noteevents_data, noteevents_sql)

    print("\n" + "=" * 70)
    print("✓ SQL files generated successfully!")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Run: psql -U admin -d hospital_db < sql/insert_scaled_patients.sql")
    print("2. Run: psql -U admin -d hospital_db < sql/insert_scaled_noteevents.sql")
    print("3. Then use load_to_mongodb.py to migrate to MongoDB")

if __name__ == "__main__":
    main()
