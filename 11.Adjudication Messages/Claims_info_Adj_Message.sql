
WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) as row_num
    FROM
        PAYOR_DW.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
        cf.IS_CURRENT = 'Y'
),
AggregatedClaimLines AS (
    SELECT
        clf.CLAIM_FACT_KEY,
        clf.REVENUE_CODE,
        clf.SERVICE_CODE,
        SUM(clf.BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(clf.PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        payor_dw.CLAIM_LINE_FACT clf
    GROUP BY
        clf.CLAIM_FACT_KEY, clf.REVENUE_CODE, clf.SERVICE_CODE,
)
SELECT
      rc.claim_hcc_id,
      s.SUPPLIER_HCC_ID,
      rc.claim_status,
      aclf_agg.TOTAL_BILLED_AMOUNT,
      aclf_agg.TOTAL_PAID_AMOUNT,
      ssd.DATE_VALUE AS Service_Start_Date ,
      am.ADJUDICATION_MESSAGE_CODE,
      dd.DATE_VALUE AS Receipt_Date,
      rc.MOST_RECENT_PROCESS_TIME
FROM
       RecentClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
 JOIN
    PAYOR_DW.SUPPLIER S ON rc.supplier_key = S.supplier_key
LEFT JOIN
    PAYOR_DW.CLAIM_LINE_FACT clf ON rc.CLAIM_FACT_KEY = clf.CLAIM_FACT_KEY
LEFT JOIN
    PAYOR_DW.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
    PAYOR_DW.DATE_DIMENSION ssd ON clf.SERVICE_START_DATE_KEY = ssd.DATE_KEY
LEFT JOIN
    PAYOR_DW.claim_line_fact_to_adjd_msg  clfam ON clf.CLAIM_LINE_FACT_KEY = clfam.CLAIM_LINE_FACT_KEY
LEFT JOIN
    PAYOR_DW.Adjudication_message am ON clfam.adjudication_message_key = am.adjudication_message_key
WHERE S.supplier_hcc_id = '1000020'
       AND dd.DATE_VALUE >= TO_TIMESTAMP('2024-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
       AND dd.DATE_VALUE < TO_TIMESTAMP('2024-10-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS')