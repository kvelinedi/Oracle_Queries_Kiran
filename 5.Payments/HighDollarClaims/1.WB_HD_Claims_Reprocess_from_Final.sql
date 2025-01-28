 ---------------------------------------------------------------------------------------------------------------------------------
 -- High Dollar Claim Came to workbasket after reprocessing from final
 ---------------------------------------------------------------------------------------------------------------------------------
   
  WITH RecentClaims AS (
    SELECT
        rc.CLAIM_HCC_ID,
        rc.CLAIM_FACT_KEY,
        rc.CLAIM_STATUS,
        rc.IS_CONVERTED,
        rc.IS_CURRENT ,
        rc.FREQUENCY_CODE ,
        rrt.trigger_code,
        rrt.trigger_desc,
        dd.DATE_VALUE,
        rc.ENTRY_TIME,
        rc.MOST_RECENT_PROCESS_TIME,
        SUM(clf.BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(clf.PAID_AMOUNT) AS TOTAL_PAID_AMOUNT,
        ROW_NUMBER() OVER (PARTITION BY rc.CLAIM_HCC_ID ORDER BY rc.MOST_RECENT_PROCESS_TIME DESC) AS ROW_NUM,
        MAX(CASE WHEN rc.CLAIM_STATUS = 'Final' THEN 1 ELSE 0 END) 
            OVER (PARTITION BY rc.CLAIM_HCC_ID) AS HAS_FINAL
    FROM
        payor_dw.claim_fact rc
    LEFT JOIN
        payor_dw.CLAIM_LINE_FACT clf ON rc.CLAIM_FACT_KEY = clf.CLAIM_FACT_KEY
    LEFT JOIN
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.CLAIM_FACT_KEY = cftrt.CLAIM_FACT_KEY
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
    LEFT JOIN
        payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY  
    WHERE  
        rc.IS_CONVERTED = 'N'
        AND rc.IS_TRIAL_CLAIM = 'N'
        AND rc.IS_CURRENT = 'Y'
        AND rrt.trigger_code IN ('44')
       -- AND rc.CLAIM_HCC_ID = '2024215001847'  --verify
        --AND rc.CLAIM_HCC_ID = '2024211005499'
        --AND rc.CLAIM_HCC_ID = '0014964898'
    GROUP BY
        rc.CLAIM_HCC_ID,
        rc.CLAIM_FACT_KEY,
        rc.CLAIM_STATUS,
        rc.IS_CONVERTED,
        rc.IS_CURRENT,
        rc.FREQUENCY_CODE,
        rrt.trigger_code,
        rrt.trigger_desc,
        dd.DATE_VALUE,
        rc.ENTRY_TIME,
        rc.MOST_RECENT_PROCESS_TIME
)
SELECT
    rcc.CLAIM_HCC_ID,
    rcc.CLAIM_FACT_KEY,
    rcc.CLAIM_STATUS,
    rcc.trigger_code,
    rcc.trigger_desc,
    rcc.FREQUENCY_CODE,
    rcc.TOTAL_BILLED_AMOUNT,
    rcc.TOTAL_PAID_AMOUNT,
    TRUNC(CURRENT_DATE) - TRUNC(rcc.DATE_VALUE) AS Claims_Aging,
    rcc.DATE_VALUE AS Receipt_Date,
    rcc.ENTRY_TIME,
    rcc.MOST_RECENT_PROCESS_TIME,
--    rcc.HAS_FINAL,
--    rcc.ROW_NUM,
    rcc.IS_CONVERTED,
    rcc.IS_CURRENT 
FROM
    RecentClaims rcc
WHERE
    rcc.ROW_NUM = 1 -- Select the most recent record
    AND rcc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review') -- Only include desired statuses
    AND rcc.HAS_FINAL = 1 -- Exclude claims with any version in 'Final' status
ORDER BY
    CLAIM_HCC_ID;   