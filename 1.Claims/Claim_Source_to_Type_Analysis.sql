WITH RecentClaims AS (
    SELECT
        csc.CLAIM_SOURCE_NAME,
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        payor_dw.claim_fact cf
        LEFT JOIN
            payor_dw.DATE_DIMENSION dd ON cf.RECEIPT_DATE_KEY = dd.DATE_KEY
        LEFT JOIN
            payor_dw.CLAIM_SOURCE_CODE csc ON cf.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
        AND dd.DATE_VALUE >= TO_TIMESTAMP('2024-09-6 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
        AND dd.DATE_VALUE < TO_TIMESTAMP('2024-09-13 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
ClaimAggregates AS (
    SELECT
        rc.CLAIM_SOURCE_NAME,
        SUM(CASE WHEN rc.CLAIM_TYPE_NAME = 'Institutional' THEN 1 ELSE 0 END) AS Institutional_Claims,
        SUM(CASE WHEN rc.CLAIM_TYPE_NAME = 'Professional' THEN 1 ELSE 0 END) AS Professional_Claims,
        COUNT(*) AS total_claims
    FROM
        RecentClaims rc
    WHERE
        rc.row_num = 1
    GROUP BY rc.CLAIM_SOURCE_NAME
),
TotalClaims AS (
    SELECT SUM(total_claims) AS grand_total FROM ClaimAggregates
)
SELECT
    COALESCE(ca.CLAIM_SOURCE_NAME, 'Total') AS CLAIM_SOURCE_NAME,
    SUM(ca.Institutional_Claims) AS Institutional_Claims,
    SUM(ca.Professional_Claims) AS Professional_Claims,
    SUM(ca.total_claims) AS total_claims,
    ROUND(100.0 * SUM(ca.total_claims) / (SELECT grand_total FROM TotalClaims), 2) AS CLAIM_SOURCE_PERCENTAGE
FROM
    ClaimAggregates ca
GROUP BY ROLLUP (ca.CLAIM_SOURCE_NAME)
HAVING
    ca.CLAIM_SOURCE_NAME IS NOT NULL OR GROUPING(ca.CLAIM_SOURCE_NAME) = 1
ORDER BY
   CASE WHEN ca.CLAIM_SOURCE_NAME IS NULL THEN 1 ELSE 0 END, 
   SUM(ca.total_claims) DESC;