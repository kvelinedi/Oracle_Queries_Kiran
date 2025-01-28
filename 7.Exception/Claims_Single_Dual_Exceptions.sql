--Report on Claims with Single and Dual Exceptions for Adjudication Improvement   
   
   WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
AggregatedClaimLines AS (
    SELECT
        CLAIM_FACT_KEY,
        COUNT(CLAIM_LINE_HCC_ID) AS Claim_lines_count
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT
    GROUP BY
        CLAIM_FACT_KEY
)
SELECT 
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    aclf_agg.Claim_lines_count,
    aclf.CLAIM_LINE_HCC_ID,
    clrrte.trigger_code AS Claim_Line_Exception_Trigger,
    clrrte.trigger_desc AS Claim_Line_Exception_Trigger_desc,
    COUNT(DISTINCT clrrte.trigger_desc) OVER (PARTITION BY rc.CLAIM_FACT_KEY) AS Unique_Exception_Trigger_Desc_Count,
    clrrt.trigger_code AS Claim_Line_Review_Trigger,
    clrrt.trigger_desc AS Claim_Line_Review_Trigger_desc,
    COUNT(DISTINCT clrrt.trigger_desc) OVER (PARTITION BY rc.CLAIM_FACT_KEY) AS Unique_Review_Trigger_Desc_Count,
    aclf.PLACE_OF_SERVICE_CODE,
    aclf.SERVICE_CODE,
    dd.DATE_VALUE AS RECEIPT_DATE,
    TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) AS day_difference_receipt,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME
FROM
    RecentClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.ALL_CLAIM_LINE_FACT aclf ON rc.CLAIM_FACT_KEY = aclf.CLAIM_FACT_KEY
LEFT JOIN
    PAYOR_DW.CLAIM_LINE_FACT_TO_EXCEPTION clfe ON aclf.CLAIM_LINE_FACT_KEY = clfe.CLAIM_LINE_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger clrrte ON clfe.REVIEW_REPAIR_TRIGGER_KEY = clrrte.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    payor_dw.CLAIM_LN_FCT_TO_REVIEW_TRIGGER clftrt ON aclf.CLAIM_LINE_FACT_KEY = clftrt.CLAIM_LINE_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger clrrt ON clftrt.REVIEW_REPAIR_TRIGGER_KEY = clrrt.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
    rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review')
    --AND aclf_agg.Claim_lines_count = 1
    --AND rc.CLAIM_HCC_ID = '2024190006169'
    AND (COUNT(DISTINCT clrrte.trigger_desc) OVER (PARTITION BY rc.CLAIM_FACT_KEY) = 1) 
    OR COUNT(DISTINCT clrrt.trigger_desc) OVER (PARTITION BY rc.CLAIM_FACT_KEY) = 1);
    AND rc.row_num = 1;

    --------------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------------

    WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
AggregatedClaimLines AS (
    SELECT
        CLAIM_FACT_KEY,
        COUNT(CLAIM_LINE_HCC_ID) AS Claim_lines_count
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT
    GROUP BY
        CLAIM_FACT_KEY
),
-- Pre-aggregating distinct counts to avoid recalculating during the main join
DistinctCounts AS (
    SELECT
        rc.CLAIM_FACT_KEY,
        COUNT(DISTINCT clrrte.trigger_desc) AS Unique_Exception_Trigger_Desc_Count,
        COUNT(DISTINCT clrrt.trigger_desc) AS Unique_Review_Trigger_Desc_Count
    FROM
        RecentClaims rc
    LEFT JOIN
        payor_dw.ALL_CLAIM_LINE_FACT aclf ON rc.CLAIM_FACT_KEY = aclf.CLAIM_FACT_KEY
    LEFT JOIN
        PAYOR_DW.CLAIM_LINE_FACT_TO_EXCEPTION clfe ON aclf.CLAIM_LINE_FACT_KEY = clfe.CLAIM_LINE_FACT_KEY
    LEFT JOIN
        payor_dw.review_repair_trigger clrrte ON clfe.REVIEW_REPAIR_TRIGGER_KEY = clrrte.REVIEW_REPAIR_TRIGGER_KEY
    LEFT JOIN
        payor_dw.CLAIM_LN_FCT_TO_REVIEW_TRIGGER clftrt ON aclf.CLAIM_LINE_FACT_KEY = clftrt.CLAIM_LINE_FACT_KEY
    LEFT JOIN
        payor_dw.review_repair_trigger clrrt ON clftrt.REVIEW_REPAIR_TRIGGER_KEY = clrrt.REVIEW_REPAIR_TRIGGER_KEY
    WHERE
        rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review')
        AND rc.row_num = 1
    GROUP BY
        rc.CLAIM_FACT_KEY
)
-- Main query now joins with pre-aggregated distinct counts
SELECT 
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    aclf_agg.Claim_lines_count,
    aclf.CLAIM_LINE_HCC_ID,
    clrrte.trigger_code AS Claim_Line_Exception_Trigger,
    clrrte.trigger_desc AS Claim_Line_Exception_Trigger_desc,
    dc.Unique_Exception_Trigger_Desc_Count,
    clrrt.trigger_code AS Claim_Line_Review_Trigger,
    clrrt.trigger_desc AS Claim_Line_Review_Trigger_desc,
    dc.Unique_Review_Trigger_Desc_Count,
    aclf.PLACE_OF_SERVICE_CODE,
    aclf.SERVICE_CODE,
    dd.DATE_VALUE AS RECEIPT_DATE,
    TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) AS day_difference_receipt,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME
FROM
    RecentClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.ALL_CLAIM_LINE_FACT aclf ON rc.CLAIM_FACT_KEY = aclf.CLAIM_FACT_KEY
LEFT JOIN
    PAYOR_DW.CLAIM_LINE_FACT_TO_EXCEPTION clfe ON aclf.CLAIM_LINE_FACT_KEY = clfe.CLAIM_LINE_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger clrrte ON clfe.REVIEW_REPAIR_TRIGGER_KEY = clrrte.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    payor_dw.CLAIM_LN_FCT_TO_REVIEW_TRIGGER clftrt ON aclf.CLAIM_LINE_FACT_KEY = clftrt.CLAIM_LINE_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger clrrt ON clftrt.REVIEW_REPAIR_TRIGGER_KEY = clrrt.REVIEW_REPAIR_TRIGGER_KEY
LEFT JOIN
    DistinctCounts dc ON rc.CLAIM_FACT_KEY = dc.CLAIM_FACT_KEY  -- Joining distinct counts
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
    rc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review')
    --AND aclf_agg.Claim_lines_count = 1
    --AND rc.CLAIM_HCC_ID = '2024190006169'
    --AND (dc.Unique_Exception_Trigger_Desc_Count + dc.Unique_Review_Trigger_Desc_Count = 1)
    AND rc.row_num = 1
    AND dc.Unique_Exception_Trigger_Desc_Count = 0 AND  dc.Unique_Review_Trigger_Desc_Count = 2