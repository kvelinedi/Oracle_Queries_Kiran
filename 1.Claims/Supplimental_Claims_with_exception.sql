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
       --AND cf.EXTERNAL_CLAIM_NUMBER = ' '
),
TriggerDataException AS (
    SELECT
        cfte.claim_fact_key,
        LISTAGG(rrte.trigger_code, ', ') WITHIN GROUP (ORDER BY rrte.trigger_code) AS exception_trigger_code,
        LISTAGG(rrte.trigger_desc, ', ') WITHIN GROUP (ORDER BY rrte.trigger_code) AS exception_trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_EXCEPTION cfte
    LEFT JOIN
        payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
    GROUP BY cfte.claim_fact_key
),
TriggerDataReview AS (
    SELECT
        cftrt.claim_fact_key,
        LISTAGG(rrt.trigger_code, ', ') WITHIN GROUP (ORDER BY rrt.trigger_code) AS review_trigger_code,
        LISTAGG(rrt.trigger_desc, ', ') WITHIN GROUP (ORDER BY rrt.trigger_code) AS review_trigger_desc
    FROM
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
    GROUP BY cftrt.claim_fact_key
)
SELECT
       rc.CLAIM_HCC_ID,
          rc.EXTERNAL_CLAIM_NUMBER,
       rc.CLAIM_STATUS,
       tde.exception_trigger_code AS exception_trigger_code,
       tde.exception_trigger_desc AS exception_trigger_desc,
       tdr.review_trigger_code AS review_trigger_code,
       tdr.review_trigger_desc AS review_trigger_desc,
       dd.DATE_VALUE AS RECEIPT_DATE,
       rc.ENTRY_TIME,
       rc.MOST_RECENT_PROCESS_TIME
FROM
    RecentClaims rc
LEFT JOIN
    TriggerDataException tde ON rc.claim_fact_key = tde.claim_fact_key
LEFT JOIN
    TriggerDataReview tdr ON rc.claim_fact_key = tdr.claim_fact_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
WHERE
    rc.row_num = 1