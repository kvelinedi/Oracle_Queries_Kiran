------------------------------------------------------------------------------------------------------------------------------------   
 --Supplier Location Required on Claim
 ------------------------------------------------------------------------------------------------------------------------------------  
WITH RecentClaims AS (
    SELECT
        cf.*,
      ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME ASC) as row_num
      --ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) as row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
--        cf.IS_CURRENT = 'Y' AND 
        cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
)
SELECT 
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    rrt.trigger_code AS Claim_Review_Trigger,  
    rrt.trigger_desc AS Claim_Review_Trigger_desc,
    rc.SI_SUPPLIER_NAME,
    ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
    rc.SI_SUPPLIER_ID,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    rc.SI_SUPPLIER_NPI,
    ashf.SUPPLIER_NPI  AS ASHF_SUPPLIER_NPI,
    rc.SI_SUPPLIER_TAX,
    TE.TAX_ID AS  ASHF_TAX_ID,
    rc.FACILITY_LOCATION_NAME AS SUBMITTED_LOCATION_NAME,
    sl.SUPPLIER_LOCATION_NAME,
    rc.FACILITY_LOCATION_ID AS SUBMITTED_LOCATION_ID,
    sl.SUPPLIER_LOCATION_HCC_ID ,
    rc.FACILITY_LOCATION_NPI AS SUBMITTED_LOCATION_NPI,
    sl.SUPPLIER_LOCATION_NPI,
    rc.MOST_RECENT_PROCESS_TIME ,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.ENTRY_TIME
FROM
    RecentClaims rc
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
     payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.claim_fact_key = cftrt.claim_fact_key
LEFT JOIN
     payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
    payor_dw.supplier ashf ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN
    payor_dw.POSTAL_ADDRESS pa ON ASHF.SUPPLIER_CORR_ADDRESS_KEY = pa.POSTAL_ADDRESS_KEY
LEFT JOIN 
    payor_dw.SUPPLIER_LOCATION sl ON rc.LOCATION_KEY = sl.SUPPLIER_LOCATION_KEY 
LEFT JOIN 
    PAYOR_DW.TAX_ENTITY TE ON ashf.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY 
WHERE
--    rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review') 
    rrt.trigger_code = '1146'
    AND rc.row_num = 1  
    --AND TE.TAX_ID = '95-1683892'
    AND rc.ENTRY_TIME >= TO_TIMESTAMP('2024-10-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    AND rc.ENTRY_TIME < TO_TIMESTAMP('2024-10-25 00:00:00', 'YYYY-MM-DD HH24:MI:SS')

----------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC ) as row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
        cf.IS_CURRENT = 'Y' AND  
--        cf.ENTRY_TIME >= TO_TIMESTAMP('2024-11-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND
--        cf.ENTRY_TIME < TO_TIMESTAMP('2024-11-13 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND
        cf.CLAIM_STATUS IN ('Needs Repair', 'Needs Review')
)
SELECT
    rc.CLAIM_HCC_ID,
    rc.SUBMITTED_SUBSCRIBER_ID,
    rc.CLAIM_STATUS,
    rrt.trigger_code AS Claim_Review_Trigger,  
    rrt.trigger_desc AS Claim_Review_Trigger_desc,
    rc.MOST_RECENT_PROCESS_TIME ,
    dd.DATE_VALUE AS RECEIPT_DATE,
--    TRUNC(CURRENT_DATE) -  TRUNC(dd.DATE_VALUE) AS day_difference_receipt,
--    CASE
--    WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 0 AND 15 THEN '0-15 Days'
--    WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 16 AND 30 THEN '16-30 Days'
--    WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 31 AND 45 THEN '31-45 Days'
--    WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 46 AND 60 THEN '46-60 Days'
--    WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) > 60 THEN '60+ Days'
--    ELSE 'Unknown'
--    END AS receipt_days_inventory,
    rc.ENTRY_TIME,
    rc.SI_SUPPLIER_NAME,
    ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
    rc.SI_SUPPLIER_NPI,
    ashf.SUPPLIER_NPI  AS ASHF_SUPPLIER_NPI,
    rc.SI_SUPPLIER_TAX,
    TE.TAX_ID AS  ASHF_TAX_ID,
    rc.SI_SUPPLIER_ID,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    rc.FACILITY_LOCATION_NAME AS SUBMITTED_LOCATION_NAME,
    sl.SUPPLIER_LOCATION_NAME,
    rc.FACILITY_LOCATION_ID AS SUBMITTED_LOCATION_ID,
    sl.SUPPLIER_LOCATION_HCC_ID ,
    rc.FACILITY_LOCATION_NPI AS SUBMITTED_LOCATION_NPI,
    sl.SUPPLIER_LOCATION_NPI
FROM
    RecentClaims rc
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
     payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.claim_fact_key = cftrt.claim_fact_key
LEFT JOIN
     payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
	payor_dw.supplier ashf ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN
	payor_dw.POSTAL_ADDRESS pa ON ASHF.SUPPLIER_CORR_ADDRESS_KEY = pa.POSTAL_ADDRESS_KEY
LEFT JOIN
	payor_dw.SUPPLIER_LOCATION sl ON rc.LOCATION_KEY = sl.SUPPLIER_LOCATION_KEY
LEFT JOIN
	PAYOR_DW.TAX_ENTITY TE ON ashf.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY
WHERE
    rrt.trigger_code = '1146'
    AND rc.row_num = 1;