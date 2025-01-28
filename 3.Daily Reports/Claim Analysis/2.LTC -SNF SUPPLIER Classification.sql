------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--2. LTC -SNF SUPPLIER Classification
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

WITH RankedClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS rn
    FROM
        CLAIM_FACT cf
    WHERE
        --cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.IS_CURRENT = 'Y'
),
AggregatedClaimLines AS (
    SELECT
        CLAIM_FACT_KEY,
        SUM(BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT
    GROUP BY
        CLAIM_FACT_KEY
),
DateFilters AS (
    SELECT 
        TO_TIMESTAMP('2024-12-23 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS Start_Date,
        TO_TIMESTAMP('2024-12-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS End_Date
    FROM dual
)
-- Main query starts here
SELECT 
    pt.CLASSIFICATION AS Supplier_Classification,
    CASE 
        WHEN GROUPING(rc.CLAIM_STATUS) = 1 THEN 'Total'
        ELSE rc.CLAIM_STATUS
    END AS CLAIM_STATUS,
    -- Total claims entered within the date range
    COUNT(CASE 
              WHEN rc.ENTRY_TIME >= (SELECT Start_Date FROM DateFilters) 
              AND rc.ENTRY_TIME < (SELECT End_Date FROM DateFilters)
              THEN 1 
          END) AS Total_Entry_Claims,
    -- Paid claims count
    SUM(CASE 
              WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 
              AND suppaydate.DATE_VALUE IS NOT NULL 
              AND rc.ENTRY_TIME >= (SELECT Start_Date FROM DateFilters)
              AND rc.ENTRY_TIME < (SELECT End_Date FROM DateFilters)
              THEN 1 ELSE 0 
          END) AS paid_claims_count,
    -- Unpaid claims count
    SUM(CASE 
              WHEN rc.CLAIM_STATUS IN ('Final', 'Denied') 
              AND suppaydate.DATE_VALUE IS NULL 
              AND (aclf_agg.TOTAL_PAID_AMOUNT IS NULL OR aclf_agg.TOTAL_PAID_AMOUNT IS NOT NULL)
              AND rc.ENTRY_TIME >= (SELECT Start_Date FROM DateFilters)
              AND rc.ENTRY_TIME < (SELECT End_Date FROM DateFilters)
              THEN 1 ELSE 0 
          END) AS adjudicated_unpaid_claims_count,
    -- Total Paid Amount
    SUM(CASE 
              WHEN suppaydate.DATE_VALUE IS NOT NULL 
              AND rc.ENTRY_TIME >= (SELECT Start_Date FROM DateFilters)
              AND rc.ENTRY_TIME < (SELECT End_Date FROM DateFilters)
              THEN aclf_agg.TOTAL_PAID_AMOUNT ELSE 0 
          END) AS total_paid_amount
FROM RankedClaims rc
LEFT JOIN AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN payor_dw.SUPPLIER ASHF ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN payor_dw.PROVIDER_TAXONOMY pt ON ashf.PRIMARY_CLASSIFICATION_KEY = pt.PROVIDER_TAXONOMY_KEY
LEFT JOIN payor_dw.PAYMENT_FACT_TO_CLAIM_FACT pftcf ON rc.CLAIM_FACT_KEY = pftcf.CLAIM_FACT_KEY
LEFT JOIN payor_dw.PAYMENT_FACT pf ON pftcf.PAYMENT_FACT_KEY = pf.PAYMENT_FACT_KEY
LEFT JOIN payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN payor_dw.DATE_DIMENSION suppaydate ON pf.PAYMENT_DATE_KEY = suppaydate.DATE_KEY
WHERE rc.rn = 1
AND pt.CLASSIFICATION IN ('Long Term Care Hospital', 'Skilled Nursing Facility')  -- Filter for the required classifications
GROUP BY GROUPING SETS ( (pt.CLASSIFICATION, rc.CLAIM_STATUS), () )
ORDER BY
    pt.CLASSIFICATION,
    rc.CLAIM_STATUS;