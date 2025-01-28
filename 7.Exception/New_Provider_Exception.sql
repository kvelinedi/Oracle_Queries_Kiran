 
  
WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) as row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
        cf.IS_CURRENT ='Y' 
        --cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
)
SELECT DISTINCT 
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    --aclf.CLAIM_LINE_HCC_ID ,
    clrrt.trigger_code AS Claim_Line_Claim_Review_Trigger,  
    clrrt.trigger_desc AS Claim_Line_Review_Trigger_desc,
    rc.SI_SUPPLIER_ID AS SUBMITTED_SUPPLIER_ID,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    TE.TAX_ID,
    scrf.CLAIM_DENY_ENABLED_IND,
    rc.PATIENT_ACCOUNT_NUMBER,
    m.MEMBER_HCC_ID,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME 
FROM
    RecentClaims rc
LEFT JOIN
     payor_dw.ALL_CLAIM_LINE_FACT aclf ON rc.CLAIM_FACT_KEY = aclf.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.CLAIM_LN_FCT_TO_REVIEW_TRIGGER clftrt  ON aclf.CLAIM_LINE_FACT_KEY = clftrt.CLAIM_LINE_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger clrrt  ON  clftrt.REVIEW_REPAIR_TRIGGER_KEY = clrrt.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
	payor_dw.supplier ashf ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN 
	PAYOR_DW.SUPPLIER_CLAIM_REVIEW_FACT scrf ON rc.SUPPLIER_KEY = scrf.SUPPLIER_KEY
LEFT JOIN
    payor_dw."MEMBER" m ON rc.MEMBER_KEY = m.MEMBER_KEY
LEFT JOIN
	PAYOR_DW.TAX_ENTITY TE ON ashf.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY
WHERE 
    rc.row_num = 1 AND 
    clrrt.trigger_desc = 'New Provider' 
    --rc.CLAIM_HCC_ID = '2024227008536'