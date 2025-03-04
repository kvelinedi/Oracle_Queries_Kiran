WITH RecentClaims AS (  
    SELECT
        cf.*,  
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num   
    FROM   
        PAYOR_DW.claim_fact cf   
    WHERE   
        cf.IS_CONVERTED = 'N'    
        AND cf.IS_TRIAL_CLAIM = 'N'  
        AND cf.IS_CURRENT = 'Y'
        AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-09-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
        AND cf.ENTRY_TIME < TO_DATE('2024-09-05 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
AggregatedClaimLines AS (
    SELECT
        CLAIM_FACT_KEY,
        SUM(BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        PAYOR_DW.ALL_CLAIM_LINE_FACT
    GROUP BY
        CLAIM_FACT_KEY
)    
SELECT
    rc.ENTRY_TIME,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.MOST_RECENT_PROCESS_TIME,
    rc.CLAIM_CUR_HCC_ID,
    rc.CLAIM_HCC_ID,
    rc.EXTERNAL_CLAIM_BATCH_NUMBER,
    rc.EXTERNAL_CLAIM_NUMBER,
    rc.CLAIM_STATUS,
    rc.CLAIM_TYPE_NAME,
    csc.CLAIM_SOURCE_NAME,
    rrt.trigger_code AS REVIEW_TRIGGER,
    rrt.trigger_desc AS REVIEW_TRIGGER_DESC,
    rrt.TRIGGER_DOMAIN_NAME AS REVIEW_TRIGGER_DOMAIN,
    rrt.POLICY_NAME AS REVIEW_TRIGGER_POLICY_NAME,
    rrte.trigger_code AS EXCEPTION_TRIGGER,
    rrte.trigger_desc AS EXCEPTION_TRIGGER_DESC,
    rrte.TRIGGER_DOMAIN_NAME AS EXCEPTION_TRIGGER_DOMAIN,
    rrte.POLICY_NAME AS EXCEPTION_TRIGGER_POLICY_NAME,
    rc.SI_SUPPLIER_NAME,
    rc.SI_SUPPLIER_ADDRESS,
    rc.SI_SUPPLIER_CITY,
    rc.SI_SUPPLIER_STATE,
    rc.SI_SUPPLIER_ZIPCODE,
    al.AUDIT_LOG_ENTRY_FACT_KEY,
    al.CLIENT_IP_ADDRESS,
    al.LOG_NOTE,
    al.MESSAGE_DESC,
    al.MESSAGE_DOMAIN_NAME,
    al.SERVER_NAME,
    al.SUBTYPE_NM,
    al.TRANSACTION_FACT_KEY,
    ua.FULL_NAME AS CLAIM_SUBMITTED_USER,
    d.DIAGNOSIS_CODE,
    d.DIAGNOSIS_LONG_DESC AS DIAGNOSIS_DESC,
    rc.FACILITY_LOCATION_NAME,
    m.MEMBER_FULL_NAME,
    m.MEMBER_HOME_ADDRESS_KEY,
    m.TELEPHONE_NUMBER,
    m.MEMBER_GENDER_CODE,
    rc.SUBMITTER_NM AS SUBMITTED_BY,
    rc.CLAIM_LEVEL_SUBMITTED_CHARGES AS BILLED_AMOUNT,
    rc.IS_ADJUSTED,
    rc.IS_AUTO_ACCIDENT,
    rc.IS_CURRENT,
    rc.IS_EMPLOYMENT_ACCIDENT,
    rc.IS_FIRST_PASS_AUTO_ADJUDICATED,
    rc.IS_FIRST_PASS_REPAIRED,
    rc.IS_FIRST_PASS_REVIEWED,
    rc.IS_OTHER_ACCIDENT,
    rc.IS_RENEWED,
    rc.IS_REPLACEMENT_OF_PROSTHESIS,
    rc.IS_TREATMENT_FOR_ORTHODONTICS,
    rc.IS_VOIDED,
    rc.IS_BLUECARD_ADJUSTMENT,
    rc.IS_POST_PAY_RECOVERY,
    clfam.ADJUDICATION_MESSAGE_KEY,
    am.ADJUDICATION_MESSAGE_CODE AS DenialCodes,
    am.ADJUDICATION_MESSAGE_DESC AS DenialReasons,
    aclf_agg.TOTAL_BILLED_AMOUNT,
    aclf_agg.TOTAL_PAID_AMOUNT,
    suppaydate.DATE_VALUE AS PaidDate,
    cif.INTEREST_AMOUNT AS InterestAmount,
    idd.DATE_VALUE AS InterestPaidDate,
    ptc.PAYMENT_TYPE_DESC
FROM
    RecentClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
    PAYOR_DW.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.CLAIM_FACT_KEY = cftrt.CLAIM_FACT_KEY
LEFT JOIN
    PAYOR_DW.review_repair_trigger rrt ON cftrt.REVIEW_REPAIR_TRIGGER_KEY = rrt.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    PAYOR_DW.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    PAYOR_DW.AUDIT_LOG_ENTRY_FACT al ON rc.AUDIT_LOG_KEY = al.AUDIT_LOG_KEY
LEFT JOIN
    PAYOR_DW.USER_ACCOUNT ua ON al.HCC_USER_ID = ua.USER_ACCOUNT_KEY
LEFT JOIN
    PAYOR_DW.DIAGNOSIS d ON rc.PRIMARY_DIAGNOSIS_CODE = d.DIAGNOSIS_CODE
LEFT JOIN
    PAYOR_DW.ACTION_TYPE_CODE atc ON al.ACTION_TYPE_CODE = atc.ACTION_TYPE_CODE
LEFT JOIN
    PAYOR_DW.MEMBER m ON rc.MEMBER_KEY = m.MEMBER_KEY
LEFT JOIN
    PAYOR_DW.CLAIM_FACT_TO_EXCEPTION cfte ON rc.CLAIM_FACT_KEY = cfte.CLAIM_FACT_KEY
LEFT JOIN
    PAYOR_DW.review_repair_trigger rrte ON cfte.REVIEW_REPAIR_TRIGGER_KEY = rrte.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    PAYOR_DW.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN    
    PAYOR_DW.ALL_CLAIM_LINE_FACT cf ON aclf_agg.CLAIM_FACT_KEY = cf.CLAIM_FACT_KEY
LEFT JOIN    
    PAYOR_DW.CLAIM_LINE_FACT_TO_ADJD_MSG clfam ON cf.CLAIM_LINE_FACT_KEY = clfam.CLAIM_LINE_FACT_KEY
LEFT JOIN    
    PAYOR_DW.ADJUDICATION_MESSAGE am ON clfam.ADJUDICATION_MESSAGE_KEY = am.ADJUDICATION_MESSAGE_KEY
LEFT JOIN 
	PAYOR_DW.PAYMENT_FACT_TO_CLAIM_FACT pftcf ON rc.CLAIM_FACT_KEY = pftcf.CLAIM_FACT_KEY  
LEFT JOIN 
	PAYOR_DW.PAYMENT_FACT pf ON pftcf.PAYMENT_FACT_KEY = pf.PAYMENT_FACT_KEY  
LEFT JOIN 
	PAYOR_DW.DATE_DIMENSION suppaydate ON pf.PAYMENT_DATE_KEY = suppaydate.DATE_KEY  
LEFT JOIN 
	PAYOR_DW.CLAIM_INTEREST_FACT cif ON cf.CLAIM_LINE_FACT_KEY = cif.CLAIM_LINE_FACT_KEY
LEFT JOIN 
	PAYOR_DW.DATE_DIMENSION IDD ON cif.PAYABLE_DATE_KEY = idd.DATE_KEY
LEFT JOIN 
	PAYOR_DW.PAYMENT_TYPE_CODE ptc ON pf.PAYMENT_TYPE_KEY = ptc.PAYMENT_TYPE_KEY
WHERE
    rc.row_num = 1
    AND CLAIM_HCC_ID = '2024246005084';


 ---------------------------------------------------------------------------------------------------------------------------------
 ---------------------------------------------------------------------------------------------------------------------------------   
    
    
    
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
),
TriggerData AS (
    SELECT
        cfte.claim_fact_key,
        rrte.trigger_code,
        rrte.trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_EXCEPTION cfte
    LEFT JOIN
        payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
    UNION ALL
    SELECT
        cftrt.claim_fact_key,
        rrt.trigger_code,
        rrt.trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
)
SELECT 
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    td.trigger_code AS TRIGGER_CODE,
    td.trigger_desc AS TRIGGER_DESC,
    rc.SI_SUPPLIER_NAME, 
    rc.SI_SUPPLIER_NPI,
    rc.SI_SUPPLIER_TAX,
    rc.FACILITY_LOCATION_NAME AS SUBMITTED_LOCATION_NAME,
    rc.FACILITY_LOCATION_ID AS SUBMITTED_LOCATION_ID,
    rc.FACILITY_LOCATION_NPI AS SUBMITTED_LOCATION_NPI,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME
FROM
    RecentClaims rc
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    TriggerData td ON rc.claim_fact_key = td.claim_fact_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
    rc.row_num = 1
    AND rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review')
    --AND td.trigger_code = '7'
    AND td.trigger_code = '1146' 
    --AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-11-04 00:00:00', 'YYYY-MM-DD HH24:MI:SS')  
    --AND cf.ENTRY_TIME < TO_TIMESTAMP('2024-11-05 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    --AND rc.SI_SUPPLIER_NAME
    --AND rc.SI_SUPPLIER_NPI
    --AND rc.SI_SUPPLIER_TAX
    --AND rc.FACILITY_LOCATION_NAME AS SUBMITTED_LOCATION_NAME
    --AND rc.FACILITY_LOCATION_ID AS SUBMITTED_LOCATION_ID
    --AND rc.FACILITY_LOCATION_NPI AS SUBMITTED_LOCATION_NPI



 ---------------------------------------------------------------------------------------------------------------------------------
 ---------------------------------------------------------------------------------------------------------------------------------   


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
        AND cf.CLAIM_STATUS IN ('Needs Repair', 'Needs Review')
        AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-11-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
        AND cf.SI_SUPPLIER_TAX IN ( '95-2977147','30-0449168','95-4651287')
),
TriggerData AS (
    SELECT
        cfte.claim_fact_key,
        rrte.trigger_code,
        rrte.trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_EXCEPTION cfte
    LEFT JOIN
        payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
    UNION ALL
    SELECT
        cftrt.claim_fact_key,
        rrt.trigger_code,
        rrt.trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
)
SELECT
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    td.trigger_code AS TRIGGER_CODE,
    td.trigger_desc AS TRIGGER_DESC,
    rc.SI_SUPPLIER_NAME,
    rc.SI_SUPPLIER_ID,
    rc.SI_SUPPLIER_NPI,
    rc.SI_SUPPLIER_TAX,
    rc.FACILITY_LOCATION_NAME AS SUBMITTED_LOCATION_NAME,
    rc.FACILITY_LOCATION_ID AS SUBMITTED_LOCATION_ID,
    rc.FACILITY_LOCATION_NPI AS SUBMITTED_LOCATION_NPI,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME
FROM
    RecentClaims rc
LEFT JOIN
    TriggerData td ON rc.claim_fact_key = td.claim_fact_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
    rc.row_num = 1
     AND td.trigger_code IS NOT NULL 
     AND td.trigger_code IN ('7','1146') ;
    