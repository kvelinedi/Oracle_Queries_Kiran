----------------------------------------------------------------------------------------------------------------------------------   
 --Exception Counts
 ---------------------------------------------------------------------------------------------------------------------------------
   
WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME ASC) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        --AND cf.IS_CURRENT = 'Y'
        AND cf.CLAIM_STATUS IN ('Needs Repair', 'Needs Review')
        AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-11-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
        AND cf.ENTRY_TIME < TO_TIMESTAMP('2024-11-13 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
        AND cf.SI_SUPPLIER_TAX IN ('95-2977147', '30-0449168', '95-4651287')
),
TriggerData AS (
    SELECT
        cfte.claim_fact_key,
        rrte.trigger_code
    FROM
        payor_dw.CLAIM_FACT_TO_EXCEPTION cfte
    LEFT JOIN
        payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
    UNION ALL
    SELECT
        cftrt.claim_fact_key,
        rrt.trigger_code
    FROM
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
)
SELECT
    rc.SI_SUPPLIER_TAX,
    SUM(CASE WHEN td.trigger_code = '7' THEN 1 ELSE 0 END) AS count_trigger_code_7,
    SUM(CASE WHEN td.trigger_code = '1146' THEN 1 ELSE 0 END) AS count_trigger_code_1146
FROM
    RecentClaims rc
LEFT JOIN
    TriggerData td ON rc.claim_fact_key = td.claim_fact_key
WHERE
    rc.row_num = 1
    AND td.trigger_code IN ('7', '1146')
GROUP BY
    rc.SI_SUPPLIER_TAX
ORDER BY
    rc.SI_SUPPLIER_TAX;
 