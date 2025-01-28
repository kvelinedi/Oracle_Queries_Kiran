   
    
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
)     
SELECT 
	rc.CLAIM_HCC_ID,
	aclf.CLAIM_LINE_HCC_ID,
    rc.CLAIM_STATUS,
    aclf.CLAIM_LINE_STATUS_CODE, 
    rc.CLAIM_TYPE_NAME,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME,
    dd.DATE_VALUE AS RECEIPT_DATE,
    am.ADJUDICATION_MESSAGE_CODE AS DenialCodes,     
    am.ADJUDICATION_MESSAGE_DESC AS DenialReasons   
FROM RecentClaims rc
LEFT JOIN payor_dw.CLAIM_LINE_FACT aclf ON rc.CLAIM_FACT_KEY = aclf.CLAIM_FACT_KEY   
LEFT JOIN payor_dw.CLAIM_LINE_FACT_TO_ADJD_MSG clfam ON aclf.CLAIM_LINE_FACT_KEY = clfam.CLAIM_LINE_FACT_KEY     
LEFT JOIN payor_dw.ADJUDICATION_MESSAGE am ON clfam.ADJUDICATION_MESSAGE_KEY = am.ADJUDICATION_MESSAGE_KEY 
LEFT JOIN payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE  
rc.claim_status = 'Denied'
AND aclf.CLAIM_LINE_STATUS_CODE = 'd'     
AND rc.row_num = 1