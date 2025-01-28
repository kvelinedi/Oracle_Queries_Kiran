-----------------------------------------------------------------------------------------------------------------------------   
--FINAL BREAK DOWN
-----------------------------------------------------------------------------------------------------------------------------   
  
   WITH AggregatedClaimLines AS (
    SELECT
        CLAIM_FACT_KEY,
        SUM(BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT
    GROUP BY
        CLAIM_FACT_KEY
), ClaimsData AS (
    SELECT
        cf.CLAIM_STATUS,
        dd.DATE_VALUE AS Receipt_Date,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS rn,
        suppaydate.DATE_VALUE AS Payment_Date,
        CASE
            WHEN suppaydate.DATE_VALUE IS NOT NULL AND aclf_agg.TOTAL_PAID_AMOUNT > 0 THEN 'PAID_CLAIM'
            WHEN suppaydate.DATE_VALUE IS NOT NULL AND aclf_agg.TOTAL_PAID_AMOUNT = 0 THEN 'ZERO_DOLLAR_PAID_CLAIM'
            WHEN suppaydate.DATE_VALUE IS NULL AND aclf_agg.TOTAL_PAID_AMOUNT IS NOT NULL THEN 'UNPAID_CLAIM'
            ELSE 'UNPAID_CLAIM'
        END AS CLAIM_CATEGORY
    FROM CLAIM_FACT cf
    JOIN DATE_DIMENSION dd ON cf.RECEIPT_DATE_KEY = dd.DATE_KEY
    LEFT JOIN payor_dw.PAYMENT_FACT_TO_CLAIM_FACT pftcf ON cf.CLAIM_FACT_KEY = pftcf.CLAIM_FACT_KEY
    LEFT JOIN payor_dw.PAYMENT_FACT pf ON pftcf.PAYMENT_FACT_KEY = pf.PAYMENT_FACT_KEY
    LEFT JOIN payor_dw.SUPPLIER ASHF ON cf.SUPPLIER_KEY = ashf.SUPPLIER_KEY
    LEFT JOIN payor_dw.DATE_DIMENSION suppaydate ON pf.PAYMENT_DATE_KEY = suppaydate.DATE_KEY
    LEFT JOIN AggregatedClaimLines aclf_agg ON cf.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
    WHERE cf.IS_CONVERTED = 'N'
      AND cf.IS_TRIAL_CLAIM = 'N'
      --AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
      AND cf.ENTRY_TIME < TO_TIMESTAMP('2024-12-26 06:00:00', 'YYYY-MM-DD HH24:MI:SS')
	  --AND dd.DATE_VALUE >= TO_TIMESTAMP('2024-09-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
	  --AND dd.DATE_VALUE <= TO_TIMESTAMP('2024-09-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
), RecentClaims AS (
    SELECT
        CLAIM_STATUS,
        CLAIM_CATEGORY,
        CURRENT_DATE - TRUNC(Receipt_Date) AS Days_Old
    FROM ClaimsData
    WHERE rn = 1
    AND CLAIM_STATUS = 'Final'
)
SELECT
    CLAIM_STATUS,
    --CLAIM_CATEGORY,
    CASE WHEN GROUPING(CLAIM_CATEGORY) =1 THEN'COMBINED' ELSE CLAIM_CATEGORY END AS CLAIM_CATEGORY,
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 0 AND 5 THEN 1 END) AS "0-5 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 6 AND 11 THEN 1 END) AS "6-11 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 12 AND 20 THEN 1 END) AS "12-20 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 21 AND 30 THEN 1 END) AS "21-30 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 31 AND 35 THEN 1 END) AS "31-35 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 36 AND 45 THEN 1 END) AS "36-45 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 46 AND 60 THEN 1 END) AS "46-60 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 61 AND 90 THEN 1 END) AS "61-90 days",
    COUNT(CASE WHEN TRUNC(Days_Old) > 90 THEN 1 END) AS "90+ days",
    COUNT(*) AS "Total"
FROM RecentClaims
--GROUP BY CLAIM_STATUS, CLAIM_CATEGORY
GROUP BY GROUPING SETS ((CLAIM_STATUS, CLAIM_CATEGORY), (CLAIM_STATUS))
ORDER BY CLAIM_STATUS, CLAIM_CATEGORY;
-----------------------------------------------------------------------------------------------------------------------------   
-----------------------------------------------------------------------------------------------------------------------------   

