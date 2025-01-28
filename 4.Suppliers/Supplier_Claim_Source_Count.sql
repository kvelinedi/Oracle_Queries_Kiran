-------------------------------------------------------------------------------------------------------------------------  
--EACH Supplier Claim SOURCE Count 
-------------------------------------------------------------------------------------------------------------------------
  
 WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
        cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
)
SELECT
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
    ptc.PAYMENT_TYPE_NAME,
    COUNT(rc.CLAIM_HCC_ID) AS total_claims,
    SUM(CASE WHEN csc.CLAIM_SOURCE_NAME = 'Electronic Claims' THEN 1 ELSE 0 END) AS Electronic_Claims_Count,
    SUM(CASE WHEN csc.CLAIM_SOURCE_NAME = 'Paper Claims' THEN 1 ELSE 0 END) AS Paper_Claims_Count,
    SUM(CASE WHEN csc.CLAIM_SOURCE_NAME = 'COBA Claims' THEN 1 ELSE 0 END) AS COBA_Claims_Count
FROM
    RecentClaims rc
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.ALL_SUPPLIER_HISTORY_FACT ashf ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN 
	payor_dw.PAYMENT_TYPE_CODE ptc ON ashf.PAYMENT_TYPE_KEY = ptc.PAYMENT_TYPE_KEY 
WHERE
    rc.row_num = 1
GROUP BY
    ashf.SUPPLIER_HCC_ID,
    ashf.SUPPLIER_NAME,
    ptc.PAYMENT_TYPE_NAME
ORDER BY  total_claims DESC;