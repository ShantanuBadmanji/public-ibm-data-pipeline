SELECT 
ROW_NUMBER() OVER(ORDER BY ID_RSSD) AS SK_ACTIVE_KEY,
ID_RSSD, NM_LGL, NM_SHORT, ENTITY_TYPE, ACT_PRIM_CD, CITY, CNTRY_NM, STATE_ABBR_NM, ZIP_CD, DOMESTIC_IND, PRIM_FED_REG,
CURRENT_TIMESTAMP AS AUDIT_TIMESTAMP 
FROM cos://us-south/<stage-bucket-name>/ACTIVE_STAGE.CSV STORED AS CSV 
order by ID_RSSD
limit 50 

INTO cos://us-south/<transform-bucket-name>/DIM_ACTIVE JOBPREFIX NONE STORED AS CSV