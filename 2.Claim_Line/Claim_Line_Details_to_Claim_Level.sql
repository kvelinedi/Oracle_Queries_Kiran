WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS row_num
    FROM
        PAYOR_DW.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
        cf.IS_CURRENT = 'Y'
),
ClaimLineRollup AS (
    SELECT
        clf.CLAIM_FACT_KEY,
        LISTAGG(DISTINCT ssd.DATE_VALUE, ', ') WITHIN GROUP (ORDER BY ssd.DATE_VALUE) AS SERVICE_START_DATES,
        LISTAGG(DISTINCT am.ADJUDICATION_MESSAGE_CODE, ', ') WITHIN GROUP (ORDER BY am.ADJUDICATION_MESSAGE_CODE) AS ADJUDICATION_MESSAGE_CODES,
        LISTAGG(DISTINCT clf.REVENUE_CODE, ', ') WITHIN GROUP (ORDER BY clf.REVENUE_CODE) AS REVENUE_CODES,
        LISTAGG(DISTINCT clf.SERVICE_CODE, ', ') WITHIN GROUP (ORDER BY clf.SERVICE_CODE) AS SERVICE_CODES,
        SUM(clf.BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(clf.PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        PAYOR_DW.CLAIM_LINE_FACT clf
    LEFT JOIN
        PAYOR_DW.DATE_DIMENSION ssd ON clf.SERVICE_START_DATE_KEY = ssd.DATE_KEY
    LEFT JOIN
        PAYOR_DW.claim_line_fact_to_adjd_msg clfam ON clf.CLAIM_LINE_FACT_KEY = clfam.CLAIM_LINE_FACT_KEY
    LEFT JOIN
        PAYOR_DW.Adjudication_message am ON clfam.adjudication_message_key = am.adjudication_message_key
    GROUP BY
        clf.CLAIM_FACT_KEY
)
SELECT
      rc.claim_hcc_id,
      s.SUPPLIER_HCC_ID,
      rc.claim_status,
      cla.TOTAL_BILLED_AMOUNT,
      cla.TOTAL_PAID_AMOUNT, 
      cla.REVENUE_CODES,
      cla.SERVICE_CODES,
      cla.SERVICE_START_DATES AS Service_Start_Dates,
      cla.ADJUDICATION_MESSAGE_CODES AS Adjudication_Message_Codes,
      dd.DATE_VALUE AS Receipt_Date,
      rc.MOST_RECENT_PROCESS_TIME
FROM
       RecentClaims rc
LEFT JOIN
    ClaimLineRollup cla ON rc.CLAIM_FACT_KEY = cla.CLAIM_FACT_KEY
JOIN
    PAYOR_DW.SUPPLIER S ON rc.supplier_key = S.supplier_key
LEFT JOIN
    PAYOR_DW.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
	rc.row_num =1
    AND S.supplier_hcc_id = '1000020'
    AND dd.DATE_VALUE >= TO_TIMESTAMP('2024-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    AND dd.DATE_VALUE < TO_TIMESTAMP('2024-10-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    --AND rc.claim_hcc_id='2024218006446';
    