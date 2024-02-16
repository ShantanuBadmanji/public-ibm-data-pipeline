-- The limits added in the queries is for demo only.
-- Initial 50 row and later 75 rows helps in check the appended rows.
-- The output is as expected. The audit_timestamp changes when a new row is added or a existing row is updated.
-- Do check if we can optimise the query :)


-- DAY 1 LOAD ACTIVITY

SELECT 
ROW_NUMBER() OVER(ORDER BY ID_RSSD) AS SK_ACTIVE_KEY,
ID_RSSD, NM_LGL, NM_SHORT, ENTITY_TYPE, ACT_PRIM_CD, CITY, CNTRY_NM, STATE_ABBR_NM, ZIP_CD, DOMESTIC_IND, PRIM_FED_REG,
CURRENT_TIMESTAMP AS AUDIT_TIMESTAMP 
FROM cos://us-south/grp-stage/clean_data.csv STORED AS CSV ORDER BY ID_RSSD LIMIT 50

INTO cos://us-south/grp-stage/dim_active JOBPREFIX NONE STORED AS CSV




-- __________SUBSEQUENT DAYS LOAD ACTIVITY_________________________
---STEP 1 create DIM_ACTIVE_SOURCE

SELECT DISTINCT
ID_RSSD, NM_LGL, NM_SHORT, ENTITY_TYPE, ACT_PRIM_CD, CITY, CNTRY_NM, STATE_ABBR_NM, ZIP_CD, DOMESTIC_IND, PRIM_FED_REG
FROM cos://us-south/grp-stage/clean_data.csv STORED AS CSV ORDER BY ID_RSSD LIMIT 75

INTO cos://us-south/grp-transform/dim_active_source JOBPREFIX NONE STORED AS CSV




---STEP 2 create DIM_ACTIVE_APP

SELECT (
  ROW_NUMBER() OVER(ORDER BY t1.ID_RSSD) + (
    SELECT MAX(SK_ACTIVE_KEY) FROM (cos://us-south/grp-stage/dim_active STORED AS CSV) as t3
  )
) AS SK_ACTIVE_KEY, t1.*
FROM (cos://us-south/grp-stage/dim_active_source STORED AS CSV) AS t1
LEFT JOIN (cos://us-south/grp-stage/dim_active STORED AS CSV) AS t2
ON t1.ID_RSSD = t2.ID_RSSD
WHERE t2.ID_RSSD IS NULL
ORDER BY ID_RSSD

INTO cos://us-south/grp-stage/dim_active_app JOBPREFIX NONE STORED AS CSV




---STEP 3 create DIM_ACTIVE_UPD

SELECT 
t2.SK_ACTIVE_KEY, 
t2.ID_RSSD,
COALESCE(t1.NM_LGL, t2.NM_LGL) AS NM_LGL, 
COALESCE(t1.NM_SHORT, t2.NM_SHORT) AS NM_SHORT,
COALESCE(t1.ENTITY_TYPE, t2.ENTITY_TYPE)  AS ENTITY_TYPE,
COALESCE(t1.ACT_PRIM_CD, t2.ACT_PRIM_CD) AS ACT_PRIM_CD,
COALESCE(t1.CITY, t2.CITY) AS CITY,
COALESCE(t1.CNTRY_NM, t2.CNTRY_NM) AS CNTRY_NM,
COALESCE(t1.STATE_ABBR_NM, t2.STATE_ABBR_NM) AS STATE_ABBR_NM, 
COALESCE(t1.ZIP_CD, t2.ZIP_CD) AS ZIP_CD,
COALESCE(t1.DOMESTIC_IND, t2.DOMESTIC_IND) AS DOMESTIC_IND, 
COALESCE(t1.PRIM_FED_REG, t2.PRIM_FED_REG) AS PRIM_FED_REG
FROM (cos://us-south/grp-stage/dim_active_source STORED AS CSV) AS t1
INNER JOIN (cos://us-south/grp-stage/dim_active STORED AS CSV) AS t2
ON t1.ID_RSSD = t2.ID_RSSD
ORDER BY ID_RSSD

INTO cos://us-south/grp-stage/dim_active_upd JOBPREFIX NONE STORED AS CSV


 

---STEP 4 create DIM_ACTIVE
SELECT t1.*, 
IF(
  md5(concat(
    t1.NM_LGL, t1.NM_SHORT, t1.ENTITY_TYPE, t1.ACT_PRIM_CD, t1.CITY, t1.CNTRY_NM, t1.STATE_ABBR_NM, t1.ZIP_CD, t1.DOMESTIC_IND, t1.PRIM_FED_REG
  )) ==
  md5(concat(
    t2.NM_LGL, t2.NM_SHORT, t2.ENTITY_TYPE, t2.ACT_PRIM_CD, t2.CITY, t2.CNTRY_NM, t2.STATE_ABBR_NM, t2.ZIP_CD, t2.DOMESTIC_IND, t2.PRIM_FED_REG
  ))
  , t2.AUDIT_TIMESTAMP,CURRENT_TIMESTAMP
) AS AUDIT_TIMESTAMP 
FROM (
  SELECT * FROM (cos://us-south/grp-stage/dim_active_app STORED AS CSV)
  UNION
  SELECT * FROM (cos://us-south/grp-stage/dim_active_upd  STORED AS CSV)
) AS t1 
LEFT JOIN  (cos://us-south/grp-stage/dim_active  STORED AS CSV) AS t2
ON t1.SK_ACTIVE_KEY = t2.SK_ACTIVE_KEY
ORDER BY SK_ACTIVE_KEY

--INTO cos://us-south/grp-stage/dim_active JOBPREFIX NONE STORED AS CSV

