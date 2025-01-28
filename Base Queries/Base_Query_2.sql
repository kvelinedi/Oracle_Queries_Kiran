 WITH RankedClaims AS (
    SELECT
  cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS rn
    FROM
        CLAIM_FACT cf
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.IS_CURRENT ='Y'
)
SELECT
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    rc.CLAIM_TYPE_NAME,
    rc.EARLIEST_ADJUDICATED_TIME,
    dd.DATE_VALUE AS Receipt_Date,
    rc.SI_SUPPLIER_NAME,
    rc.SI_SUPPLIER_ID,
    rc.SI_SUPPLIER_NPI,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME,
    rc.ADMIT_TIME ,
    dd1.DATE_VALUE AS ADMISSION_DATE 
FROM RankedClaims rc
LEFT JOIN
	payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY 
LEFT JOIN
	payor_dw.DATE_DIMENSION dd1 ON rc.ADMISSION_DATE_KEY = dd1.DATE_KEY 
WHERE
    rn = 1
    AND rc.CLAIM_HCC_ID = '2024207018230' 