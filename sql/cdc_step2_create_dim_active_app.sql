SELECT (
  ROW_NUMBER() OVER(ORDER BY t1.ID_RSSD) + (
    SELECT MAX(SK_ACTIVE_KEY) FROM (cos://us-south/<transform-bucket-name>/DIM_ACTIVE.CSV STORED AS CSV) as t3
  )
) AS SK_ACTIVE_KEY, 
t1.*, 
CURRENT_TIMESTAMP AS AUDIT_TIMESTAMP 
FROM (cos://us-south/<transform-bucket-name>/DIM_ACTIVE_SOURCE STORED AS CSV) AS t1
LEFT JOIN (cos://us-south/<transform-bucket-name>/DIM_ACTIVE.CSV STORED AS CSV) AS t2
ON t1.ID_RSSD = t2.ID_RSSD
WHERE t2.ID_RSSD IS NULL

INTO cos://us-south/<transform-bucket-name>/DIM_ACTIVE_APP JOBPREFIX NONE STORED AS CSV
