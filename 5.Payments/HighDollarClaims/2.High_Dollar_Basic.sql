
SELECT
	  rc.CLAIM_HCC_ID,
	  rc.CLAIM_FACT_KEY ,
      rc.CLAIM_STATUS,
      SUM(clf.BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
      SUM(clf.PAID_AMOUNT) AS TOTAL_PAID_AMOUNT,
      rc.MOST_RECENT_PROCESS_TIME
FROM
    payor_dw.claim_fact rc
LEFT JOIN
    payor_dw.CLAIM_LINE_FACT clf ON rc.CLAIM_FACT_KEY = clf.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.CLAIM_FACT_KEY = cftrt.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
WHERE  
	   rc.IS_CONVERTED = 'N'
       AND rc.IS_TRIAL_CLAIM = 'N'
       --AND rc.IS_CURRENT = 'Y'
       AND rrt.trigger_code IN ('44')
       AND rc.CLAIM_HCC_ID = '0014964898'
GROUP BY
	  rc.CLAIM_HCC_ID,
	  rc.CLAIM_FACT_KEY,
      rc.CLAIM_STATUS,
      rc.MOST_RECENT_PROCESS_TIME
ORDER BY 
	rc.CLAIM_HCC_ID;
-------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------

WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
      cf.IS_CONVERTED = 'N'
       AND cf.IS_TRIAL_CLAIM = 'N'
       AND cf.IS_CURRENT = 'Y'
       AND cf.CLAIM_STATUS IN ('Needs Review','Needs Repair')
),
AggregatedClaimLines AS (
    SELECT
        aclf.CLAIM_FACT_KEY,
        SUM(aclf.BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(aclf.PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT aclf
    GROUP BY
        aclf.CLAIM_FACT_KEY
)
SELECT
      rc.CLAIM_HCC_ID,
      rc.CLAIM_FACT_KEY,
      rc.CLAIM_STATUS,
      rrt.trigger_code,
      rrt.trigger_desc,
      aclf_agg.TOTAL_BILLED_AMOUNT,
      aclf_agg.TOTAL_PAID_AMOUNT,
      TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) AS Claim_Aging,
      dd.DATE_VALUE AS RECEIPT_DATE,
      CASE
        WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 0 AND 14 THEN '0-15'
        WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 15 AND 29 THEN '15-30'
        WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 30 AND 44 THEN '30-45'
        WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 45 AND 59 THEN '45-60'
        WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 60 AND 74 THEN '60-75'
        WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) BETWEEN 75 AND 89 THEN '75-90'
        WHEN TRUNC(CURRENT_DATE) - TRUNC(dd.DATE_VALUE) >= 90 THEN '90+'
        ELSE 'Unknown'
    END AS Claim_Aging_Bucket,
      rc.ENTRY_TIME,
      rc.MOST_RECENT_PROCESS_TIME
FROM
    RecentClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.CLAIM_FACT_KEY = cftrt.CLAIM_FACT_KEY
LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
    payor_dw."MEMBER" m ON rc.MEMBER_KEY = m.MEMBER_KEY
LEFT JOIN
    payor_dw.SUPPLIER s ON rc.SUPPLIER_KEY = s.SUPPLIER_KEY
WHERE
    rc.row_num = 1   
    AND rrt.trigger_code IN ('44')