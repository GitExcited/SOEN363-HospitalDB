COPY patients(row_id, subject_id, gender, dob, dod, dod_hosp, dod_ssn, expire_flag)
FROM '/docker-entrypoint-initdb.d/data/PATIENTS.csv'
DELIMITER ','
CSV HEADER;

COPY admissions(row_id, subject_id, hadm_id, admittime, dischtime, deathtime, admission_type, admission_location, discharge_location, insurance, language, religion, marital_status, ethnicity, edregtime, edouttime, diagnosis, hospital_expire_flag, has_chartevents_data)
FROM '/docker-entrypoint-initdb.d/data/ADMISSIONS.csv'
DELIMITER ','
CSV HEADER;

COPY icustays(row_id, subject_id, hadm_id, icustay_id, dbsource, first_careunit, last_careunit, first_wardid, last_wardid, intime, outtime, los)
FROM '/docker-entrypoint-initdb.d/data/ICUSTAYS.csv'
DELIMITER ','
CSV HEADER;

COPY d_icd_diagnoses(row_id, icd9_code, short_title, long_title)
FROM '/docker-entrypoint-initdb.d/data/D_ICD_DIAGNOSES.csv'
DELIMITER ','
CSV HEADER;

COPY diagnoses_icd(row_id, subject_id, hadm_id, seq_num, icd9_code)
FROM '/docker-entrypoint-initdb.d/data/DIAGNOSES_ICD.csv'
DELIMITER ','
CSV HEADER;

COPY noteevents(row_id, subject_id, hadm_id, chartdate, charttime, storetime, category, description, cgid, iserror, text)
FROM '/docker-entrypoint-initdb.d/data/NOTEEVENTS.csv'
DELIMITER ','
CSV HEADER;
