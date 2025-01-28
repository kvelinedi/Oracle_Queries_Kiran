
WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
      cf.IS_CONVERTED = 'N'
       AND cf.IS_TRIAL_CLAIM = 'N'
       AND cf.IS_CURRENT = 'Y'
--       AND cf.CLAIM_STATUS IN ('Needs Review')
--       AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-12-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
--       AND cf.ENTRY_TIME <  TO_TIMESTAMP('2024-12-30 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
--        AND cf.CLAIM_HCC_ID IN ('2024354001579')
),
AggregatedClaimLines AS (
    SELECT
        aclf.CLAIM_FACT_KEY,
        SUM(aclf.BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(aclf.PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT aclf
    GROUP BY
        aclf.CLAIM_FACT_KEY
),
TriggerDataException AS (
    SELECT
        cfte.claim_fact_key,
        LISTAGG(rrte.trigger_code, ', ') WITHIN GROUP (ORDER BY rrte.trigger_code) AS exception_trigger_code,
        LISTAGG(rrte.trigger_desc, ', ') WITHIN GROUP (ORDER BY rrte.trigger_code) AS exception_trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_EXCEPTION cfte
    LEFT JOIN
        payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
--    WHERE
--        rrte.trigger_code IN ('')
    GROUP BY cfte.claim_fact_key
),
TriggerDataReview AS (
    SELECT
        cftrt.claim_fact_key,
        LISTAGG(rrt.trigger_code, ', ') WITHIN GROUP (ORDER BY rrt.trigger_code) AS review_trigger_code,
        LISTAGG(rrt.trigger_desc, ', ') WITHIN GROUP (ORDER BY rrt.trigger_code) AS review_trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
--    WHERE
--        rrt.trigger_code IN ('')
    GROUP BY cftrt.claim_fact_key
)
SELECT
	  rc.CLAIM_HCC_ID,
       rc.CLAIM_FACT_KEY,
       rc.CLAIM_STATUS,
	  tde.exception_trigger_code AS exception_trigger_code,
	  tde.exception_trigger_desc AS exception_trigger_desc,
	  tdr.review_trigger_code AS review_trigger_code,
	  tdr.review_trigger_desc AS review_trigger_desc,   
       s.SUPPLIER_HCC_ID ,
       s.SUPPLIER_NAME ,
       s.SUPPLIER_NPI,
       s.TAX_ID AS SUPPLIER_TAX_ID,
       sl.SUPPLIER_LOCATION_HCC_ID ,
       sl.SUPPLIER_LOCATION_NPI,
       sl.SUPPLIER_LOCATION_NAME,
       m.MEMBER_HCC_ID,	
       m.MEMBER_FIRST_NAME,	
       m.MEMBER_LAST_NAME,	
       m.MEMBER_STATUS,	
       m.MEMBER_GENDER_CODE,
       aclf_agg.TOTAL_BILLED_AMOUNT,
       aclf_agg.TOTAL_PAID_AMOUNT,
       dd.DATE_VALUE AS RECEIPT_DATE,
       rc.ENTRY_TIME,
       rc.MOST_RECENT_PROCESS_TIME,
       rc.IS_CONVERTED,
       rc.IS_TRIAL_CLAIM,
       rc.IS_CURRENT
FROM
    RecentClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
    TriggerDataException tde ON rc.claim_fact_key = tde.claim_fact_key
LEFT JOIN
    TriggerDataReview tdr ON rc.claim_fact_key = tdr.claim_fact_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
    payor_dw."MEMBER" m ON rc.MEMBER_KEY = m.MEMBER_KEY
LEFT JOIN
	payor_dw.SUPPLIER s ON rc.SUPPLIER_KEY = s.SUPPLIER_KEY
LEFT JOIN
	payor_dw.SUPPLIER_LOCATION sl ON rc.LOCATION_KEY = sl.SUPPLIER_LOCATION_KEY
WHERE
	rc.row_num = 1
	--AND m.MEMBER_HCC_ID IN ('99590959G')
--    AND m.MEMBER_FIRST_NAME IN ('CATHERINE')
--    AND m.MEMBER_LAST_NAME IN ('PETERSON')
--    AND m.MEMBER_STATUS IN ('a')
--    AND m.MEMBER_GENDER_CODE IN ('F')
--    AND s.SUPPLIER_HCC_ID IN ('1001574')
--    AND s.SUPPLIER_NAME IN ('USC Care Medical Group')
--    AND s.SUPPLIER_NPI IN ('1902846306')
--    AND s.TAX_ID IN ('95-4540991')
--    AND sl.SUPPLIER_LOCATION_HCC_ID IN ('1001574-103991')
--    AND sl.SUPPLIER_LOCATION_NPI IN ('1902846306')
-- AND sl.SUPPLIER_LOCATION_NAME IN ('USC Care Medical Group - 1500 San Pablo St')
--    AND (aclf_agg.TOTAL_BILLED_AMOUNT >= 0 OR aclf_agg.TOTAL_BILLED_AMOUNT < 50000)
--    AND (aclf_agg.TOTAL_PAID_AMOUNT >= 0 OR aclf_agg.TOTAL_PAID_AMOUNT < 50000) 


-------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------
   
  --Query Updated WITH Place OF service field AND filter:--
 
WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
      cf.IS_CONVERTED = 'N'
       AND cf.IS_TRIAL_CLAIM = 'N'
       AND cf.IS_CURRENT = 'Y'
--       AND cf.CLAIM_STATUS IN ('Needs Review')
--       AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-12-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
--       AND cf.ENTRY_TIME <  TO_TIMESTAMP('2024-12-30 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
--       AND cf.CLAIM_HCC_ID IN ('0014441541','0014436295','0014505010','2024354001579')
--       AND cf.PLACE_OF_SERVICE_CODE IN ('23')
),
AggregatedClaimLines AS (
    SELECT
        aclf.CLAIM_FACT_KEY,
        SUM(aclf.BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(aclf.PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT aclf
    GROUP BY
        aclf.CLAIM_FACT_KEY
),
TriggerDataException AS (
    SELECT
        cfte.claim_fact_key,
        LISTAGG(rrte.trigger_code, ', ') WITHIN GROUP (ORDER BY rrte.trigger_code) AS exception_trigger_code,
        LISTAGG(rrte.trigger_desc, ', ') WITHIN GROUP (ORDER BY rrte.trigger_code) AS exception_trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_EXCEPTION cfte
    LEFT JOIN
        payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
    WHERE
        --rrte.trigger_code IN ('')
        rrte.trigger_code IS NOT NULL
    GROUP BY cfte.claim_fact_key
),
TriggerDataReview AS (
    SELECT
        cftrt.claim_fact_key,
        LISTAGG(rrt.trigger_code, ', ') WITHIN GROUP (ORDER BY rrt.trigger_code) AS review_trigger_code,
        LISTAGG(rrt.trigger_desc, ', ') WITHIN GROUP (ORDER BY rrt.trigger_code) AS review_trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
    WHERE
        --rrt.trigger_code IN ('')
        rrt.trigger_code IS NOT NULL
    GROUP BY cftrt.claim_fact_key
)
SELECT
      rc.CLAIM_HCC_ID,
       rc.CLAIM_FACT_KEY,
       rc.CLAIM_STATUS,
      tde.exception_trigger_code AS exception_trigger_code,
      tde.exception_trigger_desc AS exception_trigger_desc,
      tdr.review_trigger_code AS review_trigger_code,
      tdr.review_trigger_desc AS review_trigger_desc,   
       rc.PLACE_OF_SERVICE_CODE AS Place_Of_Service, 
       s.SUPPLIER_HCC_ID ,
       s.SUPPLIER_NAME ,
       s.SUPPLIER_NPI,
       s.TAX_ID AS SUPPLIER_TAX_ID,
       sl.SUPPLIER_LOCATION_HCC_ID ,
       sl.SUPPLIER_LOCATION_NPI,
       sl.SUPPLIER_LOCATION_NAME,
       PT.PROVIDER_TAXONOMY_CODE,
       pt.CLASSIFICATION AS Supplier_Classification,
       DHF.DRG_CODE ,
       m.MEMBER_HCC_ID, 
       m.MEMBER_FIRST_NAME, 
       m.MEMBER_LAST_NAME,  
       m.MEMBER_STATUS, 
       m.MEMBER_GENDER_CODE,
       aclf_agg.TOTAL_BILLED_AMOUNT,
       aclf_agg.TOTAL_PAID_AMOUNT,
       CASE
            WHEN paydt.DATE_VALUE IS NOT NULL THEN 'Paid'
            WHEN rc.CLAIM_STATUS IN ('Needs Review', 'Needs Repair') THEN 'Pended'
            WHEN rc.CLAIM_STATUS IN ('Final', 'Denied') AND paydt.DATE_VALUE IS NULL THEN 'Ready to Pay'
            ELSE 'Unknown'
        END AS "Pay Status",
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME,
    rc.IS_CONVERTED,
    rc.IS_TRIAL_CLAIM,
    rc.IS_CURRENT
FROM
    RecentClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
    TriggerDataException tde ON rc.claim_fact_key = tde.claim_fact_key
LEFT JOIN
    TriggerDataReview tdr ON rc.claim_fact_key = tdr.claim_fact_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
    payor_dw."MEMBER" m ON rc.MEMBER_KEY = m.MEMBER_KEY
LEFT JOIN
    payor_dw.SUPPLIER s ON rc.SUPPLIER_KEY = s.SUPPLIER_KEY
LEFT JOIN
    payor_dw.SUPPLIER_LOCATION sl ON rc.LOCATION_KEY = sl.SUPPLIER_LOCATION_KEY
LEFT JOIN
    payor_dw.PAYMENT_FACT_TO_CLAIM_FACT pftcf ON rc.CLAIM_FACT_KEY = pftcf.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.PAYMENT_FACT pf ON pftcf.PAYMENT_FACT_KEY = pf.PAYMENT_FACT_KEY
LEFT JOIN
    payor_dw.DATE_DIMENSION paydt ON pf.PAYMENT_DATE_KEY = paydt.DATE_KEY
LEFT JOIN 
    payor_dw.PROVIDER_TAXONOMY pt ON s.PRIMARY_CLASSIFICATION_KEY = pt.PROVIDER_TAXONOMY_KEY
LEFT JOIN 
    payor_dw.DRG DHF ON rc.DRG_KEY = DHF.DRG_KEY 
WHERE
    rc.row_num = 1   
    --AND m.MEMBER_HCC_ID IN ('97557081F','99590959G')
--    AND s.SUPPLIER_HCC_ID IN ('1000019','1001574')
--    AND m.MEMBER_FIRST_NAME IN ('CATHERINE')
--    AND m.MEMBER_LAST_NAME IN ('PETERSON')
--    AND m.MEMBER_STATUS IN ('a')
--    AND m.MEMBER_GENDER_CODE IN ('F')
--    AND s.SUPPLIER_NAME IN ('USC Care Medical Group')
--    AND s.SUPPLIER_NPI IN ('1902846306')
--    AND s.TAX_ID IN ('95-4540991')
--    AND sl.SUPPLIER_LOCATION_HCC_ID IN ('1001574-103991')
--    AND sl.SUPPLIER_LOCATION_NPI IN ('1902846306')
--    AND sl.SUPPLIER_LOCATION_NAME IN ('USC Care Medical Group - 1500 San Pablo St')
--    AND (aclf_agg.TOTAL_BILLED_AMOUNT >= 0 OR aclf_agg.TOTAL_BILLED_AMOUNT < 50000)
--    AND (aclf_agg.TOTAL_PAID_AMOUNT >= 0 OR aclf_agg.TOTAL_PAID_AMOUNT < 50000)
    --AND DHF.DRG_CODE 
    AND PT.PROVIDER_TAXONOMY_CODE IN ('291U00000X')
    AND pt.CLASSIFICATION IN ('Clinical Medical Laboratory')