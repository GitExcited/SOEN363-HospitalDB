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
