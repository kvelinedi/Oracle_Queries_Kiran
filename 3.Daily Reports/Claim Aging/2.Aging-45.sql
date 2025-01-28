--------------------------------------------------------------------------------------------------------------------------------------
--CLAIM AGING REPORT - 45
----------------------------------------------------------------------------------------------------------------------------------------
WITH ClaimsData AS (
    SELECT
        cf.CLAIM_STATUS,
        dd.DATE_VALUE AS Receipt_Date,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS rn
    FROM CLAIM_FACT cf
    JOIN DATE_DIMENSION dd ON cf.RECEIPT_DATE_KEY = dd.DATE_KEY
    WHERE cf.IS_CONVERTED = 'N'
      AND cf.IS_TRIAL_CLAIM = 'N'
      AND CF.IS_CURRENT ='Y'
      --AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
      AND cf.ENTRY_TIME < TO_TIMESTAMP('2024-12-26 06:00:00', 'YYYY-MM-DD HH24:MI:SS')
), RecentClaims AS (
    SELECT
        CLAIM_STATUS,
        CURRENT_DATE - TRUNC(Receipt_Date) AS Days_Old
    FROM ClaimsData
    WHERE rn = 1
)
SELECT
    COALESCE(CLAIM_STATUS, 'Total') AS CLAIM_STATUS,
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 0 AND 15 THEN 1 END) AS "0-15 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 16 AND 30 THEN 1 END) AS "16-30 days",
    COUNT(CASE WHEN TRUNC(Days_Old) BETWEEN 31 AND 45 THEN 1 END) AS "31-45 days",
    COUNT(CASE WHEN TRUNC(Days_Old) > 45 THEN 1 END) AS "45+ days",
    COUNT(*) AS "Total"
FROM RecentClaims
GROUP BY ROLLUP(CLAIM_STATUS)
ORDER BY CLAIM_STATUS;