SELECT jsonb_agg(
  jsonb_build_object(
    'subject_id', noteevents.subject_id,
    'hadm_id', noteevents.hadm_id,
    'chartdate', noteevents.chartdate,
    'charttime', noteevents.charttime,
    'storetime', noteevents.storetime,
    'category', noteevents.category,
    'description', noteevents.description,
    'cgid', noteevents.cgid,
    'iserror', noteevents.iserror,
    'text', noteevents.text
  )
)
FROM noteevents;









