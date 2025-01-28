-------------------------------------------------------------------------------------------------------------------------------
--Original Version
-------------------------------------------------------------------------------------------------------------------------------
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
        --AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-11-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
        --AND cf.ENTRY_TIME < TO_TIMESTAMP('2024-11-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
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
SELECT
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    dd.DATE_VALUE AS Receipt_Date,
    rc.PATIENT_ACCOUNT_NUMBER,
    rc.MEDICAL_RECORD_NUMBER,
    rc.SI_SUPPLIER_NAME,
    rc.SI_SUPPLIER_ID,
    rc.SI_SUPPLIER_NPI,
    rc.SI_SUPPLIER_ADDRESS,
    rc.SI_SUPPLIER_CITY,
    rc.SI_SUPPLIER_STATE,
    rc.SI_SUPPLIER_ZIPCODE,
    ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    PT.PROVIDER_TAXONOMY_CODE ,
    pt.CLASSIFICATION AS Supplier_Classification,
    rc.CLAIM_LEVEL_SUBMITTED_CHARGES AS BILLED_AMOUNT,
    aclf_agg.TOTAL_PAID_AMOUNT,
    suppaydate.DATE_VALUE AS Payment_Date,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME
    --suppaydate.DATE_NAME_1 
FROM RankedClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
	payor_dw.PAYMENT_FACT_TO_CLAIM_FACT pftcf ON rc.CLAIM_FACT_KEY = pftcf.CLAIM_FACT_KEY
LEFT JOIN
	payor_dw.PAYMENT_FACT pf ON pftcf.PAYMENT_FACT_KEY = pf.PAYMENT_FACT_KEY
LEFT JOIN
	payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY 
LEFT JOIN
	payor_dw.SUPPLIER ASHF ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY 
LEFT JOIN
	payor_dw.PROVIDER_TAXONOMY pt ON ashf.PRIMARY_CLASSIFICATION_KEY = pt.PROVIDER_TAXONOMY_KEY
LEFT JOIN
	payor_dw.DATE_DIMENSION suppaydate ON pf.PAYMENT_DATE_KEY = suppaydate.DATE_KEY
WHERE
    rn = 1
    --AND rc.CLAIM_HCC_ID = '2024183003176'
    AND rc.CLAIM_STATUS NOT IN ('Final')
    
    
    AND ashf.SUPPLIER_HCC_ID = '1000281';
    AND suppaydate.DATE_VALUE IS NULL ;
    AND pt.CLASSIFICATION = 'Long Term Care Hospital';

    --AND suppaydate.DATE_VALUE IS NOT NULL;
    --AND PT.PROVIDER_TAXONOMY_CODE = '282E00000X'
    --AND pt.CLASSIFICATION = 'Long Term Care Hospital';
    --AND PT.PROVIDER_TAXONOMY_CODE = '314000000X'
    --AND pt.CLASSIFICATION = 'Skilled Nursing Facility';