-----------------------------------------------------------------------------------------------------------------------------------
--Updated Clean Date
----------------------------------------------------------------------------------------------------------------------------------- 

WITH RankedClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME DESC) AS rn
    FROM
        CLAIM_FACT cf
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
        AND cf.IS_CURRENT = 'Y'
        AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
AggregatedClaimLines AS (
    SELECT  
        aclf.CLAIM_FACT_KEY,
        --cleanclaimdd.DATE_VALUE AS ORG_CLEAN_CLAIM_DATE,
        COALESCE(cleanclaimdd.DATE_VALUE, dd.DATE_VALUE) AS MOD_CLEAN_CLAIM_DATE,  -- Use COALESCE to fill with receipt DATE
        SUM(aclf.BILLED_AMOUNT) AS TOTAL_BILLED_AMOUNT,
        SUM(aclf.PAID_AMOUNT) AS TOTAL_PAID_AMOUNT
    FROM
        payor_dw.ALL_CLAIM_LINE_FACT aclf
    LEFT JOIN
       RankedClaims RC ON aclf.CLAIM_FACT_KEY = rc.CLAIM_FACT_KEY
  	LEFT JOIN
   	 payor_dw.DATE_DIMENSION cleanclaimdd ON aclf.CLEAN_CLAIM_LINE_DATE_KEY = cleanclaimdd.DATE_KEY
   	 LEFT JOIN
     payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
    GROUP BY
        aclf.CLAIM_FACT_KEY, COALESCE(cleanclaimdd.DATE_VALUE, dd.DATE_VALUE)
)
SELECT
    rc.CLAIM_HCC_ID,
    rc.CLAIM_STATUS,
    rc.CLAIM_TYPE_NAME,
    csc.CLAIM_SOURCE_NAME,
    rc.ENTRY_TIME,
    --aclf_agg.ORG_CLEAN_CLAIM_DATE,
    aclf_agg.MOD_CLEAN_CLAIM_DATE,
    --aclf_agg.TOTAL_PAID_AMOUNT,
    --COALESCE(aclf_agg.CLEAN_CLAIM_DATE, dd.DATE_VALUE) AS CLEAN_CLAIM_DATE_modified,  -- Use COALESCE to fill with receipt DATE
    --TRUNC(CURRENT_DATE) - TRUNC(COALESCE(aclf_agg.CLEAN_CLAIM_DATE, dd.DATE_VALUE)) AS day_difference_clean_claim, -- New column for days difference with clean claim date
    dd.DATE_VALUE AS RECEIPT_DATE,
    TRUNC(CURRENT_DATE) -  TRUNC(dd.DATE_VALUE) AS day_difference_receipt,
    rc.MOST_RECENT_PROCESS_TIME
FROM RankedClaims rc
LEFT JOIN
    AggregatedClaimLines aclf_agg ON rc.CLAIM_FACT_KEY = aclf_agg.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.PAYMENT_FACT_TO_CLAIM_FACT pftcf ON rc.CLAIM_FACT_KEY = pftcf.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.PAYMENT_FACT pf ON pftcf.PAYMENT_FACT_KEY = pf.PAYMENT_FACT_KEY
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
    payor_dw.DATE_DIMENSION suppaydate ON pf.PAYMENT_DATE_KEY = suppaydate.DATE_KEY
WHERE
    rn = 1
    AND rc.CLAIM_STATUS IN ('Final', 'Denied')
    AND suppaydate.DATE_VALUE IS NOT NULL
    AND rc.CLAIM_HCC_ID = '2024218000996';




---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------   
--Based on our previous call with Jeff, the below given information is how we were told to calculate a clean claim.
--
--Definition of Clean Claim - All information is entered into the system. Clean Claim Date is on the Line Item
--
--Auto Adjudication - First pass, Final and Denied, No User Intervention.
--
--The dashboard has been updated with this logic as well and Hari had asked to share this information to all so that we can cross check the condition by which we identify a clean claim in our OPS report.
--The definition which i have given on the claims dashboard documentation is as below:
--
--A Clean Claim is a claim that is either in Final or Denied status and has been adjudicated correctly on the first pass. The Total Clean Claim Count shows the total number of clean claims in the inventory based on the selected criteria, such as date range or claim status. This metric helps measure how many claims were processed accurately and efficiently without requiring manual intervention.
--
--DAX Formula used to flag a claim as clean claim:
--clean_claim_flag =
--IF(
--   UPDATED_CLAIMS[IS_FIRST_PASS_AUTO_ADJUDICATED] = "Y"
--   && (UPDATED_CLAIMS[CLAIM_STATUS] = "Final" || UPDATED_CLAIMS[CLAIM_STATUS] = "Denied"),
--   "YES",
--   "NO"
--)
--This formula creates a flag where it marks a claim as a clean claim by checking if the claim is in Final or Denied state and is also auto adjudicated in the first pass.
---------------------------------------------------------------------------------------------------------------------------------------------   
---------------------------------------------------------------------------------------------------------------------------------------------   
    
--FOR the claims IN HRP. the claim line LEVEL, clean claim DATE field IS empty.
--WHERE the clean claim line DATE IS available IN the VIEW INPUT form.
--
--does health edge OR changes IN configrations  help us TO GET the clean claim DATE TO claim line LEVEL.
--
--AT present clean claim line DATE IS available ONLY FOR very few claims IN prod.
--
--
--IF CLAIM RECEIVES INTO SYSTEM AND THEN GOES TO NEEDS REPAIR STATUS DUE TO INSUFFICENT INFORMATION FROM SUPPLIER.
--AT PRESENT WHAT ARE WE DOING,
--HOLDING CLAIM IN  NEEDS REPAIR STATUS AND UPDATE INFORMATION ONCE RECEIVED FROM SUPPLIER.
--FROM THEN MOVING INTO ADJUDICATION?
--OR,  ARE WE DENYING THESE CLAIM'S AND Supplier sending back new claim with proper information?
--
--A claim might have 10 claim lines are we adjudicating and paying all lines at a time or paying few lines first and few next?
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------