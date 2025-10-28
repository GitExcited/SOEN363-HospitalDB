SELECT
    p.subject_id,
    p.gender,
    p.dob,
    COUNT(i.icustay_id) AS icu_stay_count
FROM PATIENTS p
JOIN ICUSTAYS i ON p.subject_id = i.subject_id
GROUP BY p.subject_id , p.gender, p.dob
HAVING COUNT(i.icustay_id) > 1;