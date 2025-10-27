DROP TABLE IF EXISTS diagnoses_icd CASCADE;
DROP TABLE IF EXISTS noteevents CASCADE;
DROP TABLE IF EXISTS icustays CASCADE;
DROP TABLE IF EXISTS admissions CASCADE;
DROP TABLE IF EXISTS patients CASCADE;
DROP TABLE IF EXISTS d_icd_diagnoses CASCADE;

CREATE TABLE patients (
    row_id INTEGER PRIMARY KEY,
    subject_id INTEGER UNIQUE NOT NULL,
    gender VARCHAR(1),
    dob TIMESTAMP,
    dod TIMESTAMP,
    dod_hosp TIMESTAMP,
    dod_ssn TIMESTAMP,
    expire_flag INTEGER
);

CREATE TABLE admissions (
    row_id INTEGER PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    hadm_id INTEGER UNIQUE NOT NULL,
    admittime TIMESTAMP,
    dischtime TIMESTAMP,
    deathtime TIMESTAMP,
    admission_type VARCHAR(50),
    admission_location VARCHAR(50),
    discharge_location VARCHAR(50),
    insurance VARCHAR(50),
    language VARCHAR(50),
    religion VARCHAR(50),
    marital_status VARCHAR(50),
    ethnicity VARCHAR(100),
    edregtime TIMESTAMP,
    edouttime TIMESTAMP,
    diagnosis VARCHAR(255),
    hospital_expire_flag INTEGER,
    has_chartevents_data INTEGER,
    FOREIGN KEY (subject_id) REFERENCES patients(subject_id)
);

CREATE TABLE icustays (
    row_id INTEGER PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    hadm_id INTEGER NOT NULL,
    icustay_id INTEGER UNIQUE NOT NULL,
    dbsource VARCHAR(20),
    first_careunit VARCHAR(50),
    last_careunit VARCHAR(50),
    first_wardid INTEGER,
    last_wardid INTEGER,
    intime TIMESTAMP,
    outtime TIMESTAMP,
    los NUMERIC(10, 4),
    FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
    FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id)
);

CREATE TABLE noteevents (
    row_id INTEGER PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    hadm_id INTEGER NOT NULL,
    chartdate DATE,
    charttime TIMESTAMP,
    storetime TIMESTAMP,
    category VARCHAR(50),
    description VARCHAR(255),
    cgid INTEGER,
    iserror INTEGER,
    text TEXT,
    FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
    FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id)
);

CREATE TABLE d_icd_diagnoses (
    row_id INTEGER PRIMARY KEY,
    icd9_code VARCHAR(10) UNIQUE NOT NULL,
    short_title VARCHAR(50),
    long_title VARCHAR(255)
);

CREATE TABLE diagnoses_icd (
    row_id INTEGER PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    hadm_id INTEGER NOT NULL,
    seq_num INTEGER,
    icd9_code VARCHAR(10),
    FOREIGN KEY (subject_id) REFERENCES patients(subject_id),
    FOREIGN KEY (hadm_id) REFERENCES admissions(hadm_id),
    FOREIGN KEY (icd9_code) REFERENCES d_icd_diagnoses(icd9_code)
);

CREATE INDEX idx_patients_subject_id ON patients(subject_id);
CREATE INDEX idx_admissions_subject_id ON admissions(subject_id);
CREATE INDEX idx_admissions_hadm_id ON admissions(hadm_id);
CREATE INDEX idx_icustays_subject_id ON icustays(subject_id);
CREATE INDEX idx_icustays_hadm_id ON icustays(hadm_id);
CREATE INDEX idx_noteevents_subject_id ON noteevents(subject_id);
CREATE INDEX idx_noteevents_hadm_id ON noteevents(hadm_id);
CREATE INDEX idx_diagnoses_subject_id ON diagnoses_icd(subject_id);
CREATE INDEX idx_diagnoses_hadm_id ON diagnoses_icd(hadm_id);
CREATE INDEX idx_diagnoses_icd9_code ON diagnoses_icd(icd9_code);
