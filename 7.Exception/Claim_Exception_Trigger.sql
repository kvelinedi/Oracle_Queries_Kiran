 ------------------------------------------------------------------------------------------------------------------------------------   
 --Supplier NOT FOUND claims inventory
 ------------------------------------------------------------------------------------------------------------------------------------  
  WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME ASC) as row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
--        cf.IS_CURRENT ='Y' AND 
        cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
)
SELECT 
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    rrte.trigger_code AS EXCEPTION_TRIGGER,
    rrte.trigger_desc AS EXCEPTION_TRIGGER_DESC,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME ,
    rc.SI_SUPPLIER_NAME,
    ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
    rc.SI_SUPPLIER_NPI,
    ashf.SUPPLIER_NPI  AS ASHF_SUPPLIER_NPI,
    rc.SI_SUPPLIER_TAX,
    TE.TAX_ID AS  ASHF_TAX_ID,
    rc.SI_SUPPLIER_ID,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    rc.SI_SUPPLIER_ADDRESS,
    rc.SI_SUPPLIER_CITY ,
    rc.SI_SUPPLIER_STATE ,
    rc.SI_SUPPLIER_COUNTRY ,
    rc.SI_SUPPLIER_ZIPCODE,
    PA.ADDRESS_LINE AS ASHF_SUPPLIER_ADDRESS,
    PA.CITY_NAME AS ASHF_SUPPLIER_CITY,
    PA.STATE_CODE AS ASHF_SUPPLIER_STATE ,
    PA.COUNTY_CODE AS ASHF_SUPPLIER_COUNTRY,
    PA.ZIP_CODE AS ASHF_SUPPLIER_ZIPCODE
FROM
    RecentClaims rc
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.CLAIM_FACT_TO_EXCEPTION cfte ON rc.claim_fact_key = cfte.claim_fact_key
LEFT JOIN
    payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
    payor_dw.supplier ashf ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN
    payor_dw.POSTAL_ADDRESS pa ON ASHF.SUPPLIER_CORR_ADDRESS_KEY = pa.POSTAL_ADDRESS_KEY
LEFT JOIN 
    PAYOR_DW.TAX_ENTITY TE ON ashf.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY 
LEFT JOIN 
    PAYOR_DW.SUPPLIER_LOCATION_HIST_FACT SLHF ON ashf.SUPPLIER_KEY = SLHF.SUPPLIER_KEY 
WHERE
    --rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review') AND
    rrte.trigger_code = '7' AND 
    --TE.TAX_ID = '95-1683892' AND 
    rc.ENTRY_TIME >= TO_TIMESTAMP('2024-09-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND 
    rc.ENTRY_TIME < TO_TIMESTAMP('2024-09-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND 
    --rc.CLAIM_HCC_ID = '2024177001128' AND 
    rc.row_num = 1;

-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------

WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) as row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
        cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
)
SELECT DISTINCT 
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    rc.CLAIM_TYPE_NAME,
    csc.CLAIM_SOURCE_NAME,
    rc.ENTRY_TIME,
    dd.DATE_VALUE AS RECEIPT_DATE,
    TRUNC(CURRENT_DATE) -  TRUNC(dd.DATE_VALUE) AS day_difference,
    rc.MOST_RECENT_PROCESS_TIME,
    rrte.trigger_code AS EXCEPTION_TRIGGER,
    rrte.trigger_desc AS EXCEPTION_TRIGGER_DESC,
    rrte.TRIGGER_DOMAIN_NAME AS EXCEPTION_TRIGGER_DOMAIN,
    rrte.POLICY_NAME AS EXCEPTION_TRIGGER_POLICY_NAME,
    rc.SI_SUPPLIER_NAME,
    rc.SI_SUPPLIER_NPI, 
    rc.SI_SUPPLIER_ID,
    rc.SI_SUPPLIER_ADDRESS,
    --rc.SI_SUPPLIER_CITY ,
    --rc.SI_SUPPLIER_STATE ,
    --rc.SI_SUPPLIER_COUNTRY ,
    --rc.SI_SUPPLIER_ZIPCODE,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    ashf.SUPPLIER_NPI  AS ASHF_SUPPLIER_NPI
    --rc.SUBMITTED_PAY_TO_ADDRESS_KEY,
    --pa.ADDRESS_LINE AS SUBMITTED_PAY_TO_ADDRESS,
    --pa.CITY_NAME ,
    --pa.STATE_CODE ,
    --pa.COUNTRY_NAME ,
    --pa.ZIP_CODE 
FROM
    RecentClaims rc
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.CLAIM_FACT_TO_EXCEPTION cfte ON rc.claim_fact_key = cfte.claim_fact_key
LEFT JOIN
    payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN 
	payor_dw.ALL_SUPPLIER_HISTORY_FACT ashf ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY 
LEFT JOIN 
	payor_dw.POSTAL_ADDRESS pa ON rc.SUBMITTED_PAY_TO_ADDRESS_KEY = pa.POSTAL_ADDRESS_KEY 
WHERE
    rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review') AND 
    --rrte.trigger_desc  = 'Supplier could not be identified' AND
    --rrte.trigger_code = '7' AND
    dd.DATE_VALUE >= TO_TIMESTAMP('2024-06-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND
    dd.DATE_VALUE <= TO_TIMESTAMP('2024-07-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND
    --rc.claim_hcc_id='2024176001458' AND
    rc.row_num = 1;