SELECT DISTINCT (p.subject_id, a.hadm_id)
FROM patients p
JOIN admissions a ON p.subject_id = a.subject_id
JOIN noteevents n ON a.hadm_id = n.hadm_id
WHERE n.category IN ('Radiology', 'ECG');