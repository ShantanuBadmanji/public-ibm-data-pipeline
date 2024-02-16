SELECT 
    `#ID_RSSD` as ID_RSSD,
    DATE(concat(
regexp_extract(D_DT_START,'(\\d+)/(\\d+)/(\\d+)*',3),"-",
regexp_extract(D_DT_START,'(\\d+)/(\\d+)/(\\d+)*',1),"-",
regexp_extract(D_DT_START,'(\\d+)/(\\d+)/(\\d+)*',2))) as D_DT_START,
    DATE(concat(
regexp_extract(D_DT_END,'(\\d+)/(\\d+)/(\\d+)*',3),"-",
regexp_extract(D_DT_END,'(\\d+)/(\\d+)/(\\d+)*',1),"-",
regexp_extract(D_DT_END,'(\\d+)/(\\d+)/(\\d+)*',2))) as D_DT_END,
    BHC_IND,BROAD_REG_CD,CHTR_AUTH_CD,CHTR_TYPE_CD,FBO_4C9_IND,FHC_IND,FUNC_REG,INSUR_PRI_CD,
    MBR_FHLBS_IND,MBR_FRS_IND,SEC_RPTG_STATUS,EST_TYPE_CD,BANK_CNT,BNK_TYPE_ANALYS_CD,
    DATE(concat(
regexp_extract(D_DT_EXIST_CMNC,'(\\d+)/(\\d+)/(\\d+)*',3),"-",
regexp_extract(D_DT_EXIST_CMNC,'(\\d+)/(\\d+)/(\\d+)*',1),"-",
regexp_extract(D_DT_EXIST_CMNC,'(\\d+)/(\\d+)/(\\d+)*',2))) as D_DT_EXIST_CMNC,
    DATE(concat(
regexp_extract(D_DT_EXIST_TERM,'(\\d+)/(\\d+)/(\\d+)*',3),"-",
regexp_extract(D_DT_EXIST_TERM,'(\\d+)/(\\d+)/(\\d+)*',1),"-",
regexp_extract(D_DT_EXIST_TERM,'(\\d+)/(\\d+)/(\\d+)*',2))) as D_DT_EXIST_TERM,
    FISC_YREND_MMDD,
    DATE(concat(
regexp_extract(D_DT_INSUR,'(\\d+)/(\\d+)/(\\d+)*',3),"-",
regexp_extract(D_DT_INSUR,'(\\d+)/(\\d+)/(\\d+)*',1),"-",
regexp_extract(D_DT_INSUR,'(\\d+)/(\\d+)/(\\d+)*',2))) as  D_DT_INSUR,
    DATE(concat(
regexp_extract(D_DT_OPEN,'(\\d+)/(\\d+)/(\\d+)*',3),"-",
regexp_extract(D_DT_OPEN,'(\\d+)/(\\d+)/(\\d+)*',1),"-",
regexp_extract(D_DT_OPEN,'(\\d+)/(\\d+)/(\\d+)*',2))) as  D_DT_OPEN,
	FNCL_SUB_HOLDER,FNCL_SUB_IND,IBA_GRNDFTHR_IND,IBF_IND,ID_RSSD_HD_OFF,MJR_OWN_MNRTY,
    TRIM(NM_LGL) as NM_LGL,
    TRIM(NM_SHORT) as NM_SHORT,
    NM_SRCH_CD,ORG_TYPE_CD,REASON_TERM_CD,CNSRVTR_CD,
    TRIM(ENTITY_TYPE) as ENTITY_TYPE,
    AUTH_REG_DIST_FRS,ACT_PRIM_CD,
    TRIM(CITY) as CITY,
    TRIM(CNTRY_NM) as CNTRY_NM,
    ID_CUSIP,
    TRIM(STATE_ABBR_NM) as STATE_ABBR_NM,
    PLACE_CD,STATE_CD,STATE_HOME_CD,
    TRIM(STREET_LINE1) as STREET_LINE1,
    TRIM(STREET_LINE2) as STREET_LINE2,
    TRIM(ZIP_CD) as ZIP_CD,
    ID_THRIFT,
    TRIM(ID_THRIFT_HC) as ID_THRIFT_HC,
    TRIM(DOMESTIC_IND) as DOMESTIC_IND,
    ID_ABA_PRIM,ID_FDIC_CERT,ID_NCUA,COUNTY_CD,DIST_FRS,ID_OCC,CNTRY_CD,DT_END,DT_EXIST_CMNC,
    DT_EXIST_TERM,DT_INSUR,DT_OPEN,DT_START,ID_TAX,
    TRIM(PROV_REGION) as PROV_REGION,
    TRIM(URL) as URL,
    SLHC_IND,SLHC_TYPE_IND,
    TRIM(PRIM_FED_REG) as PRIM_FED_REG,
    STATE_INC_CD,CNTRY_INC_CD,
    TRIM(STATE_INC_ABBR_NM) as STATE_INC_ABBR_NM,
    CNTRY_INC_NM,
    TRIM(ID_LEI) as ID_LEI,
    IHC_IND

FROM cos://us-south/<source-bucket-name>/<path-to-source-file> STORED AS CSV 

INTO cos://us-south/<stage-bucket-name>/<path-to-target-file> JOBPREFIX NONE STORED AS CSV
