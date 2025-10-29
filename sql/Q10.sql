SELECT * 
FROM noteevents n
WHERE n.category = 'Discharge summary'
LIMIT 10;


/*alternative */
select n.chartdate, n.storetime, n.category, n.description, n."text" 
from noteevents n
where n.category = 'Discharge summary'
limit 10;