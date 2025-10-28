UPDATE NOTEEVENTS 
SET TEXT = 'Patient presents with chest pain. Chest X-ray ordered to evaluate lung fields and rule out pneumonia.'
WHERE ROW_ID = 2 AND CATEGORY = 'Radiology';
