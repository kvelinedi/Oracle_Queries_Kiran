-----------------------------------------------------------------------------------------------------------------------------
--Adjuducation AND Autoadjudication percentage
-----------------------------------------------------------------------------------------------------------------------------   

WITH FilteredClaims AS (
    SELECT
        cf.CLAIM_STATUS,
        cf.IS_FIRST_PASS_AUTO_ADJUDICATED,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) as row_num
    FROM
        payor_dw.claim_fact cf
        LEFT JOIN payor_dw.DATE_DIMENSION dd ON cf.RECEIPT_DATE_KEY = dd.DATE_KEY
    WHERE
        cf.IS_CONVERTED = 'N' 
        AND cf.IS_TRIAL_CLAIM = 'N' 
        AND CF.IS_CURRENT = 'Y'
        --AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-11-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
        AND cf.ENTRY_TIME < TO_TIMESTAMP('2024-12-26 06:00:00', 'YYYY-MM-DD HH24:MI:SS')
)
SELECT
    COALESCE(CLAIM_STATUS, 'Overall') AS CLAIM_STATUS,
    COUNT(*) AS status_count,
    SUM(CASE WHEN CLAIM_STATUS IN ('Final', 'Denied') THEN 1 ELSE 0 END) AS adjudicated_count,
    ROUND((SUM(CASE WHEN CLAIM_STATUS IN ('Final', 'Denied') THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(CASE WHEN CLAIM_STATUS != 'Rejected' THEN 1 END), 0), 2) AS adjudication_percentage,
    SUM(CASE WHEN IS_FIRST_PASS_AUTO_ADJUDICATED = 'Y' AND CLAIM_STATUS IN ('Final', 'Denied') THEN 1 ELSE 0 END) AS auto_adjudicated_count,
    ROUND((SUM(CASE WHEN CLAIM_STATUS IN ('Final', 'Denied') AND IS_FIRST_PASS_AUTO_ADJUDICATED = 'Y' THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(CASE WHEN CLAIM_STATUS != 'Rejected' THEN 1 END), 0), 2) AS auto_adjudication_percentage
FROM
    FilteredClaims
WHERE
    row_num = 1
GROUP BY
    ROLLUP(CLAIM_STATUS);