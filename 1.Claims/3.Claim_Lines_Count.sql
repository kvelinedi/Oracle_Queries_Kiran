    
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
),
AggregatedClaimLines AS (
    SELECT
        aclf.CLAIM_FACT_KEY,
        COUNT(aclf.CLAIM_LINE_HCC_ID) AS Total_Claim_lines_count
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT aclf
    GROUP BY
        aclf.CLAIM_FACT_KEY
)
SELECT
    rc.CLAIM_HCC_ID,
    aclf_agg.Total_Claim_lines_count,
    CASE
        WHEN aclf_agg.Total_Claim_lines_count IS NULL THEN 'No Lines'
        WHEN aclf_agg.Total_Claim_lines_count BETWEEN 0 AND 5 THEN '0-5 lines'
        WHEN aclf_agg.Total_Claim_lines_count BETWEEN 6 AND 10 THEN '6-10 lines'
        WHEN aclf_agg.Total_Claim_lines_count BETWEEN 11 AND 15 THEN '11-15 lines'
        WHEN aclf_agg.Total_Claim_lines_count BETWEEN 16 AND 20 THEN '16-20 lines'
        ELSE '20+ lines'
    END AS line_count_range,
    rc.CLAIM_STATUS,
    rc.CLAIM_TYPE_NAME,
    csc.CLAIM_SOURCE_NAME,
    dd.DATE_VALUE AS Receipt_Date
FROM RecentClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
    row_num = 1
    AND rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review');