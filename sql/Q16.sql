SELECT DISTINCT (p.subject_id, p.gender, a.hadm_id)
FROM patients p
INNER JOIN admissions a ON p.subject_id = a.subject_id
INNER JOIN noteevents n ON a.hadm_id = n.hadm_id
WHERE n.category = 'Radiology';
