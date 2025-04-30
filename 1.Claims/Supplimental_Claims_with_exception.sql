 WITH RecentClaims AS (
    SELECT
        rc.CLAIM_HCC_ID,
        --rc.CLAIM_FACT_KEY,
        rc.CLAIM_STATUS,
        csc.CLAIM_SOURCE_NAME,
        rc.IS_CONVERTED,
        rc.IS_CURRENT ,
        dd.DATE_VALUE,
        rc.ENTRY_TIME,
        rc.MOST_RECENT_PROCESS_TIME,
        ROW_NUMBER() OVER (PARTITION BY rc.CLAIM_HCC_ID ORDER BY rc.MOST_RECENT_PROCESS_TIME DESC) AS ROW_NUM,
        MAX(CASE WHEN rc.CLAIM_STATUS = 'Final' THEN 1 ELSE 0 END) 
            OVER (PARTITION BY rc.CLAIM_HCC_ID) AS HAS_FINAL
    FROM
        payor_dw.claim_fact rc
    JOIN
        payor_dw.CLAIM_LINE_FACT clf ON rc.CLAIM_FACT_KEY = clf.CLAIM_FACT_KEY
    JOIN
        payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY  
    LEFT JOIN
    	payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
    WHERE  
        rc.IS_CONVERTED = 'N'
        AND rc.IS_TRIAL_CLAIM = 'N'
        AND rc.IS_CURRENT = 'Y'
        --AND rc.CLAIM_HCC_ID = '2024179012955'  --verify
)
SELECT
    rcc.CLAIM_HCC_ID,
    --rcc.CLAIM_FACT_KEY,
    rcc.CLAIM_STATUS,
    RCC.CLAIM_SOURCE_NAME,
    rcc.DATE_VALUE AS Receipt_Date,
    rcc.ENTRY_TIME,
    rcc.MOST_RECENT_PROCESS_TIME,
    rcc.IS_CONVERTED,
    rcc.IS_CURRENT 
FROM
    RecentClaims rcc
WHERE
    rcc.ROW_NUM = 1 -- Select the most recent record
    AND rcc.HAS_FINAL = 1-- Exclude claims with any version in 'Final' status
    AND RCC.CLAIM_SOURCE_NAME = 'COBA Claims'
    AND rcc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review') -- Only include desired statuses
ORDER BY
    CLAIM_HCC_ID; 
