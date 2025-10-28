SELECT DISTINCT
    p.subject_id,
    p.gender,
    p.dob,
    i.icustay_id,
    i.first_careunit AS transfer_from,
    i.last_careunit AS trasnfer_to
FROM patients p
JOIN icustays i ON p.subject_id = i.subject_id
WHERE i.first_careunit != i.last_careunit;
