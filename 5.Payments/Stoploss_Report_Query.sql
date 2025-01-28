WITH ClaimStatusCheck AS (
    SELECT
        rcrc.MEMBER_KEY,
        rcrc.CLAIM_HCC_ID,
        COUNT(CASE WHEN rcrc.CLAIM_STATUS IN ('Final', 'Denied') THEN 1 END) AS FinalOrDeniedCount
    FROM
        PAYOR_DW.CLAIM_FACT rcrc
    GROUP BY
        rcrc.MEMBER_KEY,
        rcrc.CLAIM_HCC_ID
),
FilteredClaims AS (
    SELECT
        m.MEMBER_FULL_NAME AS "Subscriber Name",
        m.MEMBER_HCC_ID AS "Subscriber ID",
        s.SUPPLIER_HCC_ID AS "Pay To Provider ID",
        s.SUPPLIER_NPI AS "Pay To Provider NPI",
        s.SUPPLIER_NAME AS "Pay To Provider Name",
        rcrc.TYPE_OF_BILL_CODE AS "Type Of Bill",
        clf.SERVICE_CODE,
        ncf.SUBMITTED_NDC_CODE AS "NDC Number",
        clf.REVENUE_CODE,
        DHF.DRG_CODE,
        mods.MODIFIERS AS "Modifiers",
        rcrc.PLACE_OF_SERVICE_CODE AS "Place Of Service",
        rcrc.PRIMARY_DIAGNOSIS_CODE AS "Primary Diagnosis",
        clf.UNIT_COUNT,
        d5.DATE_VALUE AS "Admit Date",
        d1.DATE_VALUE AS "Statement Period From",
        d2.DATE_VALUE AS "Statement Period To",
        d6.DATE_VALUE AS "Service Start Date",
        d7.DATE_VALUE AS "Service End Date",
        rcrc.CLAIM_HCC_ID AS "Claim ID",
        rcrc.CLAIM_STATUS AS "Claim Status",
        COALESCE(ascpos.ADMIT_STATUS_NAME, asctob.ADMIT_STATUS_NAME) AS ADMIT_STATUS_NAME,
        clf.CLAIM_LINE_HCC_ID AS "Claim Line ID",
        clf.BILLED_AMOUNT AS "Amount Billed",
        CASE
            WHEN paydt.DATE_VALUE IS NOT NULL THEN cpf.PAYABLE_AMOUNT
            ELSE clf.PAID_AMOUNT
        END AS "Final Paid Amount",
        cif.INTEREST_AMOUNT as pf_Interest_amount,
        CASE
            WHEN paydt.DATE_VALUE IS NOT NULL THEN 'Paid'
            WHEN rcrc.CLAIM_STATUS IN ('Needs Repair', 'Needs Review') AND paydt.DATE_VALUE IS NULL THEN 'Pended'
            WHEN rcrc.CLAIM_STATUS IN ('Final', 'Denied') AND paydt.DATE_VALUE IS NULL THEN 'Ready to Pay'
            ELSE 'Unknown'
        END AS "Pay Status",
        pf.PAYMENT_NUMBER AS "Check Number",
        paydt.DATE_VALUE AS "Payment Date",
        CASE
            WHEN bnhf.BENEFIT_NETWORK_NAME IS NOT NULL THEN 'Par'
            ELSE 'Non-Par'
        END AS "PAR Status",
        PT.PROVIDER_TAXONOMY_CODE,
        pt.CLASSIFICATION AS Supplier_Classification
    FROM
        PAYOR_DW.CLAIM_FACT rcrc
    LEFT JOIN ClaimStatusCheck csc ON rcrc.CLAIM_HCC_ID = csc.CLAIM_HCC_ID
        AND rcrc.MEMBER_KEY = csc.MEMBER_KEY
    LEFT JOIN payor_dw.CLAIM_LINE_FACT clf ON rcrc.CLAIM_FACT_KEY = clf.CLAIM_FACT_KEY
    LEFT JOIN payor_dw."MEMBER" m ON rcrc.MEMBER_KEY = m.MEMBER_KEY
    LEFT JOIN payor_dw.SUPPLIER s ON rcrc.SUPPLIER_KEY = s.SUPPLIER_KEY
    LEFT JOIN payor_dw.DATE_DIMENSION d1 ON rcrc.STATEMENT_START_DATE_KEY = d1.DATE_KEY
    LEFT JOIN payor_dw.DATE_DIMENSION d2 ON rcrc.STATEMENT_END_DATE_KEY = d2.DATE_KEY
    LEFT JOIN payor_dw.DATE_DIMENSION d5 ON rcrc.ADMISSION_DATE_KEY = d5.DATE_KEY
    LEFT JOIN payor_dw.DATE_DIMENSION d6 ON clf.SERVICE_START_DATE_KEY = d6.DATE_KEY
    LEFT JOIN payor_dw.DATE_DIMENSION d7 ON clf.SERVICE_END_DATE_KEY = d7.DATE_KEY
    LEFT JOIN payor_dw.CLAIM_PAYABLE_FACT cpf ON clf.CLAIM_LINE_FACT_KEY = cpf.CLAIM_LINE_FACT_KEY
    LEFT JOIN payor_dw.PMT_FACT_TO_CLM_PAYABLE_FACT pftcpf ON cpf.CLAIM_PAYABLE_FACT_KEY = pftcpf.CLAIM_PAYABLE_FACT_KEY
    LEFT JOIN payor_dw.PAYMENT_FACT pf ON pftcpf.PAYMENT_FACT_KEY = pf.PAYMENT_FACT_KEY
    LEFT JOIN payor_dw.DATE_DIMENSION paydt ON pf.PAYMENT_DATE_KEY = paydt.DATE_KEY
    LEFT JOIN payor_dw.PAYMENT_STATUS_CODE psc ON pf.PAYMENT_STATUS_CODE = psc.PAYMENT_STATUS_CODE
    LEFT JOIN PAYOR_DW.SUPPLIER_HIST_TO_BNFT_NETWORK shtbn ON s.SUPPLIER_HISTORY_FACT_KEY = shtbn.SUPPLIER_HISTORY_FACT_KEY
    LEFT JOIN PAYOR_DW.BENEFIT_NETWORK_HISTORY_FACT bnhf ON shtbn.BENEFIT_NETWORK_KEY = bnhf.BENEFIT_NETWORK_KEY
    LEFT JOIN payor_dw.CLAIM_LN_FACT_TO_NDC_CODE_INFO cfnc ON clf.CLAIM_LINE_FACT_KEY = cfnc.CLAIM_LINE_FACT_KEY
    LEFT JOIN payor_dw.NDC_CODE_INFO_FACT ncf ON cfnc.NDC_CODE_INFO_KEY = ncf.NDC_CODE_INFO_KEY
    LEFT JOIN payor_dw.DRG DHF ON rcrc.DRG_KEY = DHF.DRG_KEY
    LEFT JOIN payor_dw.PROVIDER_TAXONOMY pt ON s.PRIMARY_CLASSIFICATION_KEY = pt.PROVIDER_TAXONOMY_KEY
    LEFT JOIN payor_dw.CLAIM_INTRST_FCT_TO_CLM_PAYBLE ciftcp ON cpf.CLAIM_PAYABLE_FACT_KEY = ciftcp.CLAIM_PAYABLE_FACT_KEY
	LEFT JOIN payor_dw.CLAIM_INTEREST_FACT cif ON cif.CLAIM_INTEREST_FACT_KEY  = ciftcp.CLAIM_INTEREST_FACT_KEY
	LEFT JOIN payor_dw.PLACE_OF_SERVICE pos ON rcrc.PLACE_OF_SERVICE_CODE = pos.PLACE_OF_SERVICE_CODE 
	LEFT JOIN payor_dw.ADMIT_STATUS_CODE ascpos ON pos.ADMIT_STATUS_CODE = ascpos.ADMIT_STATUS_CODE 
	LEFT JOIN payor_dw.TYPE_OF_BILL tob ON rcrc.TYPE_OF_BILL_CODE = tob.TYPE_OF_BILL_CODE 
	LEFT JOIN payor_dw.ADMIT_STATUS_CODE asctob ON tob.ADMIT_STATUS_CODE = asctob.ADMIT_STATUS_CODE 
    LEFT JOIN (
        SELECT
            CLFM.CLAIM_LINE_FACT_KEY,
            LISTAGG(CLFM.MODIFIER_CODE, ', ') WITHIN GROUP (ORDER BY CLFM.MODIFIER_CODE) AS MODIFIERS
        FROM
            payor_dw.CLAIM_LINE_FACT_TO_MODIFIER CLFM
        GROUP BY
            CLFM.CLAIM_LINE_FACT_KEY
    ) mods ON clf.CLAIM_LINE_FACT_KEY = mods.CLAIM_LINE_FACT_KEY
    WHERE
        rcrc.IS_CONVERTED = 'N'
        AND rcrc.IS_TRIAL_CLAIM = 'N'
        AND (
            (csc.FinalOrDeniedCount > 0 AND cpf.PAYABLE_AMOUNT IS NOT NULL)
            OR (csc.FinalOrDeniedCount = 0 AND rcrc.IS_CURRENT = 'Y')
        )
        AND d6.DATE_VALUE BETWEEN TO_TIMESTAMP('2024-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP('2024-12-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
        AND paydt.DATE_VALUE < TO_TIMESTAMP('2025-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
AggregatedClaims AS (
    SELECT
        "Subscriber ID",
        SUM("Final Paid Amount") AS TotalFinalPaidAmount
    FROM
        FilteredClaims
    GROUP BY
        "Subscriber ID"
    HAVING
        SUM("Final Paid Amount") > 650000
)
SELECT
    fc.*
FROM
    FilteredClaims fc
JOIN
    AggregatedClaims ac
ON
    fc."Subscriber ID" = ac."Subscriber ID"
ORDER BY
    fc."Subscriber ID",
    fc."Claim ID",
    fc."Payment Date",
    fc."Claim Line ID";