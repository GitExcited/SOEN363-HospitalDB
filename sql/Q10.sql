SELECT n.chartdate, n.storetime, n.category, n.description, n."text" 
FROM noteevents n
WHERE n.category = 'Discharge summary'
LIMIT 10;