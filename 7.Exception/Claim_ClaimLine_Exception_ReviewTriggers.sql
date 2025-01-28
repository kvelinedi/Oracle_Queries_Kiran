
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
    aclf.CLAIM_LINE_HCC_ID,
    rc.EXTERNAL_CLAIM_NUMBER,
    rc.CLAIM_STATUS,
    rc.CLAIM_TYPE_NAME,
    csc.CLAIM_SOURCE_NAME,
    TRUNC(CURRENT_DATE) -  TRUNC(dd.DATE_VALUE) AS day_difference,
    rc.ENTRY_TIME,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.MOST_RECENT_PROCESS_TIME,
    rrt.trigger_code AS Claim_Review_Trigger,  
    rrt.trigger_desc AS Claim_Review_Trigger_desc,
    rrt.TRIGGER_DOMAIN_NAME AS Claim_Review_Trigger_Domain,
    rrt.POLICY_NAME AS Claim_Review_Trigger_Policy_Name,        
    clrrt.trigger_code AS Claim_Line_Review_Trigger,  
    clrrt.trigger_desc AS Claim_Line_Review_Trigger_desc,
    clrrt.TRIGGER_DOMAIN_NAME AS Claim_Line_Review_Trigger_Domain,
    clrrt.POLICY_NAME AS Claim_Line_Review_Trigger_Policy_Name,        
    rrte.trigger_code AS Claim_Exception_Trigger,         
    rrte.trigger_desc AS Claim_Exception_Trigger_desc,
    rrte.TRIGGER_DOMAIN_NAME AS Claim_Exception_Trigger_Domain,      
    rrte.POLICY_NAME AS Claim_Exception_Trigger_Policy_Name,          
    clrrte.trigger_code AS Claim_Line_Claim_Exception_Trigger,  
    clrrte.trigger_desc AS Claim_Line_Exception_Trigger_desc,
    clrrte.TRIGGER_DOMAIN_NAME AS Claim_Line_Exception_Trigger_Domain,
    clrrte.POLICY_NAME AS Claim_Line_Exception_Trigger_Policy_Name,     
    rc.SI_SUPPLIER_NAME,
    rc.SI_SUPPLIER_ADDRESS,
    rc.SI_SUPPLIER_CITY,
    rc.SI_SUPPLIER_STATE,
    rc.SI_SUPPLIER_ZIPCODE,
    d.DIAGNOSIS_CODE,
    d.DIAGNOSIS_LONG_DESC AS DIAGNOSIS_DESC,
    m.MEMBER_FULL_NAME,
    rc.SUBMITTER_NM AS SUBMITTED_BY,
    rc.CLAIM_LEVEL_SUBMITTED_CHARGES AS BILLED_AMOUNT,
    rc.IS_ADJUSTED
FROM
    RecentClaims rc
LEFT JOIN
    payor_dw.ALL_CLAIM_LINE_FACT aclf ON rc.CLAIM_FACT_KEY = aclf.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.CLAIM_FACT_KEY = cftrt.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger rrt ON cftrt.REVIEW_REPAIR_TRIGGER_KEY = rrt.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.AUDIT_LOG_ENTRY_FACT al ON rc.AUDIT_LOG_KEY = al.AUDIT_LOG_KEY
LEFT JOIN
    payor_dw.USER_ACCOUNT ua ON al.HCC_USER_ID = ua.USER_ACCOUNT_KEY
LEFT JOIN
    payor_dw.DIAGNOSIS d ON rc.PRIMARY_DIAGNOSIS_CODE = d.DIAGNOSIS_CODE
LEFT JOIN
    payor_dw.ACTION_TYPE_CODE atc ON al.ACTION_TYPE_CODE = atc.ACTION_TYPE_CODE
LEFT JOIN
    payor_dw.MEMBER m ON rc.MEMBER_KEY = m.MEMBER_KEY
LEFT JOIN
    payor_dw.CLAIM_FACT_TO_EXCEPTION cfte ON rc.CLAIM_FACT_KEY = cfte.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger rrte ON cfte.REVIEW_REPAIR_TRIGGER_KEY = rrte.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    payor_dw.CLAIM_LN_FCT_TO_REVIEW_TRIGGER clftrt  ON aclf.CLAIM_LINE_FACT_KEY = clftrt.CLAIM_LINE_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger clrrt  ON  clftrt.REVIEW_REPAIR_TRIGGER_KEY = clrrt.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    PAYOR_DW.CLAIM_LINE_FACT_TO_EXCEPTION clfe ON aclf.CLAIM_LINE_FACT_KEY = clfe.CLAIM_LINE_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger clrrte ON clfe.REVIEW_REPAIR_TRIGGER_KEY = clrrte.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
    rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review') 
    --AND rrt.trigger_code ='22'
    --AND rrte.trigger_code = '22'
    --AND clrrt.trigger_code ='22'
    AND clrrte.trigger_code = '22'
    --AND dd.DATE_VALUE >= TO_TIMESTAMP('2024-06-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    --AND dd.DATE_VALUE <= TO_TIMESTAMP('2024-07-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    AND rc.row_num = 1;