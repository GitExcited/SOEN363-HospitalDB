SELECT jsonb_agg(  
  jsonb_build_object (
  
    'subject_id', patients.subject_id,
    'gender', patients.gender,
    'dob', patients.dob,
    'dod', patients.dod,
    'dod_hosp', patients.dod_hosp,
    'dod_ssn', patients.dod_ssn,
    'expire_flag', patients.expire_flag,
  
    'admissions', ( 
      SELECT jsonb_agg(
        jsonb_build_object(
          'hadm_id', admissions.hadm_id,
          'admittime', admissions.admittime,
          'dischtime', admissions.dischtime,
          'deathtime', admissions.deathtime,
          'admission_type', admissions.admission_type,
          'admission_location', admissions.admission_location,
          'discharge_location', admissions.discharge_location,
          'insurance', admissions.insurance,
          'language', admissions.language,
          'religion', admissions.religion,
          'marital_status', admissions.marital_status,
          'ethnicity', admissions.ethnicity,
          'edregtime', admissions.edregtime,
          'edouttime', admissions.edouttime,
          'diagnosis', admissions.diagnosis,
          'hospital_expire_flag', admissions.hospital_expire_flag,
          'has_chartevents_data', admissions.has_chartevents_data,

          'icustays', (
            SELECT jsonb_agg(
              jsonb_build_object(
                'icustay_id', icustays.icustay_id,
                'dbsource', icustays.dbsource,
                'first_careunit', icustays.first_careunit,
                'last_careunit', icustays.last_careunit,
                'first_wardid', icustays.first_wardid,
                'last_wardid', icustays.last_wardid,
                'intime', icustays.intime,
                'outtime', icustays.outtime,
                'los', icustays.los
              )
            )
            FROM icustays
            WHERE icustays.hadm_id = admissions.hadm_id
          ),

          'diagnoses_icd', (
            SELECT jsonb_agg(
              jsonb_build_object(
                'seq_num', diagnoses_icd.seq_num,
                'icd9_code', diagnoses_icd.icd9_code,
                'short_title', d_icd_diagnoses.short_title,
                'long_title', d_icd_diagnoses.long_title
              )
            )
            FROM diagnoses_icd
            JOIN d_icd_diagnoses ON diagnoses_icd.icd9_code = d_icd_diagnoses.icd9_code
            WHERE diagnoses_icd.hadm_id = admissions.hadm_id
          )  
        )
      )
      FROM admissions
      WHERE admissions.subject_id = patients.subject_id
    )
  )
)
FROM patients;

 











