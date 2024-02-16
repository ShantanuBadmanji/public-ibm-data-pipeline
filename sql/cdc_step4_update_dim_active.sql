SELECT * FROM (cos://us-south/<transform-bucket-name>/DIM_ACTIVE_APP STORED AS CSV)
UNION ALL
SELECT * FROM (cos://us-south/<transform-bucket-name>/DIM_ACTIVE_UPD  STORED AS CSV)

INTO cos://us-south/<transform-bucket-name>/DIM_ACTIVE JOBPREFIX NONE STORED AS CSV
