------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--3. CLAIM PAYMENT DAY RANGES
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 WITH RankedClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS rn,
        CURRENT_DATE - TRUNC(dd.DATE_VALUE) AS Days_Old -- Use DATE_DIMENSION for Receipt Date
    FROM
        CLAIM_FACT cf
    LEFT JOIN payor_dw.DATE_DIMENSION dd ON cf.RECEIPT_DATE_KEY = dd.DATE_KEY -- Correct Join to get the receipt date
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
)
-- Main query starts here
SELECT 
    CASE 
        WHEN GROUPING(rc.CLAIM_STATUS) = 1 THEN 'Total'
        ELSE rc.CLAIM_STATUS
    END AS CLAIM_STATUS,

    -- Total Claims (no date filter)
    COUNT(1) AS "Total Claims",
--	CASE 
--	    WHEN GROUPING(rc.CLAIM_STATUS) = 1 THEN 'Total'
--	    ELSE rc.CLAIM_STATUS
--	END AS CLAIM_STATUS,
--	
--	-- Total Claims calculation
--	COUNT(CASE 
--	          WHEN rc.CLAIM_FACT_KEY IS NOT NULL 
--	          THEN 1 ELSE NULL 
--	      END) AS Total_Claims,

    -- Total Paid Claims (no date filter)
    SUM(CASE 
              WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 
              AND suppaydate.DATE_VALUE IS NOT NULL 
              THEN 1 ELSE 0 
          END) AS "Total Paid Claims",

    -- Total Paid Percentage (no date filter)
    ROUND(
        CASE 
            WHEN COUNT(1) = 0 THEN 0
            ELSE
                (SUM(CASE 
                        WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 
                        AND suppaydate.DATE_VALUE IS NOT NULL 
                        THEN 1 ELSE 0 
                     END) * 100.0) 
                / COUNT(1)
        END, 2) AS "Total Paid %",

    -- Claim count, Paid claim count, and Paid claim percentage for 0-15 days
    COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 0 AND 15 THEN 1 END) AS "0-15 days Count",
    SUM(CASE WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 AND suppaydate.DATE_VALUE IS NOT NULL AND TRUNC(rc.Days_Old) BETWEEN 0 AND 15 THEN 1 ELSE 0 END) AS "0-15 days Paid Count",
    ROUND(
        CASE WHEN COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 0 AND 15 THEN 1 END) = 0 THEN 0
        ELSE (SUM(CASE WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 AND suppaydate.DATE_VALUE IS NOT NULL AND TRUNC(rc.Days_Old) BETWEEN 0 AND 15 THEN 1 ELSE 0 END) * 100.0) 
             / COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 0 AND 15 THEN 1 END) 
        END, 2) AS "0-15 days Paid %",

    -- Claim count, Paid claim count, and Paid claim percentage for 16-30 days
    COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 16 AND 30 THEN 1 END) AS "16-30 days Count",
    SUM(CASE WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 AND suppaydate.DATE_VALUE IS NOT NULL AND TRUNC(rc.Days_Old) BETWEEN 16 AND 30 THEN 1 ELSE 0 END) AS "16-30 days Paid Count",
    ROUND(
        CASE WHEN COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 16 AND 30 THEN 1 END) = 0 THEN 0
        ELSE (SUM(CASE WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 AND suppaydate.DATE_VALUE IS NOT NULL AND TRUNC(rc.Days_Old) BETWEEN 16 AND 30 THEN 1 ELSE 0 END) * 100.0) 
             / COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 16 AND 30 THEN 1 END) 
        END, 2) AS "16-30 days Paid %",

    -- Claim count, Paid claim count, and Paid claim percentage for 31-45 days
    COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 31 AND 45 THEN 1 END) AS "31-45 days Count",
    SUM(CASE WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 AND suppaydate.DATE_VALUE IS NOT NULL AND TRUNC(rc.Days_Old) BETWEEN 31 AND 45 THEN 1 ELSE 0 END) AS "31-45 days Paid Count",
    ROUND(
        CASE WHEN COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 31 AND 45 THEN 1 END) = 0 THEN 0
        ELSE (SUM(CASE WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 AND suppaydate.DATE_VALUE IS NOT NULL AND TRUNC(rc.Days_Old) BETWEEN 31 AND 45 THEN 1 ELSE 0 END) * 100.0) 
             / COUNT(CASE WHEN TRUNC(rc.Days_Old) BETWEEN 31 AND 45 THEN 1 END) 
        END, 2) AS "31-45 days Paid %",

    -- Claim count, Paid claim count, and Paid claim percentage for 45+ days
    COUNT(CASE WHEN TRUNC(rc.Days_Old) > 45 THEN 1 END) AS "45+ days Count",
    SUM(CASE WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 AND suppaydate.DATE_VALUE IS NOT NULL AND TRUNC(rc.Days_Old) > 45 THEN 1 ELSE 0 END) AS "45+ days Paid Count",
    ROUND(
        CASE WHEN COUNT(CASE WHEN TRUNC(rc.Days_Old) > 45 THEN 1 END) = 0 THEN 0
        ELSE (SUM(CASE WHEN aclf_agg.TOTAL_PAID_AMOUNT >= 0 AND suppaydate.DATE_VALUE IS NOT NULL AND TRUNC(rc.Days_Old) > 45 THEN 1 ELSE 0 END) * 100.0) 
             / COUNT(CASE WHEN TRUNC(rc.Days_Old) > 45 THEN 1 END) 
        END, 2) AS "45+ days Paid %"
FROM RankedClaims rc
LEFT JOIN AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN payor_dw.SUPPLIER ASHF ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN payor_dw.PROVIDER_TAXONOMY pt ON ashf.PRIMARY_CLASSIFICATION_KEY = pt.PROVIDER_TAXONOMY_KEY
LEFT JOIN payor_dw.PAYMENT_FACT_TO_CLAIM_FACT pftcf ON rc.CLAIM_FACT_KEY = pftcf.CLAIM_FACT_KEY
LEFT JOIN payor_dw.PAYMENT_FACT pf ON pftcf.PAYMENT_FACT_KEY = pf.PAYMENT_FACT_KEY
LEFT JOIN payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY -- Join to get receipt date
LEFT JOIN payor_dw.DATE_DIMENSION suppaydate ON pf.PAYMENT_DATE_KEY = suppaydate.DATE_KEY
WHERE rc.rn = 1
AND rc.ENTRY_TIME >= TO_TIMESTAMP('2024-12-23 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
AND rc.ENTRY_TIME < TO_TIMESTAMP('2024-12-24 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
GROUP BY GROUPING SETS ( (rc.CLAIM_STATUS), () )
ORDER BY
    rc.CLAIM_STATUS;