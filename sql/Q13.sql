SELECT (
    count(diag.icd9_code) AS diagnosis_count, 
    d.short_title , 
    d.long_title) 
FROM diagnoses_icd  diag
LEFT JOIN d_icd_diagnoses d
ON d.icd9_code = diag.icd9_code 
GROUP BY(d.icd9_code,d.short_title, d.long_title)
ORDER BY diagnosis_count  desc
LIMIT 5;