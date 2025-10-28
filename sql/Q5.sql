SELECT 
    p.subject_id,
    p.gender,
    p.dob,
    a.insurance
FROM PATIENTS AS p 
JOIN ADMISSIONS as A 
ON p.subject_id = a.subject_id
WHERE insurance = 'Private'
