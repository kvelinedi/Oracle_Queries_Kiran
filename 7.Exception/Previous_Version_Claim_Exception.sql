 ------------------------------------------------------------------------------------------------------------------------------
 WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME ASC) AS row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N'
        AND cf.IS_TRIAL_CLAIM = 'N'
--        AND cf.IS_CURRENT = 'Y'
        AND cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
ClaimWithTriggers AS (
    SELECT
        rc.CLAIM_HCC_ID,
        rc.EXTERNAL_CLAIM_NUMBER,
        rc.CLAIM_STATUS,
        rc.SI_SUPPLIER_NAME,
        ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
        rc.SI_SUPPLIER_NPI,
        ashf.SUPPLIER_NPI AS ASHF_SUPPLIER_NPI,
        rc.SI_SUPPLIER_ID,
        ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
        rc.FACILITY_LOCATION_NAME AS SUBMITTED_LOCATION_NAME,
        sl.SUPPLIER_LOCATION_NAME,
        rc.FACILITY_LOCATION_ID AS SUBMITTED_LOCATION_ID,
        sl.SUPPLIER_LOCATION_HCC_ID,
        rc.FACILITY_LOCATION_NPI AS SUBMITTED_LOCATION_NPI,
        sl.SUPPLIER_LOCATION_NPI,
        rc.SI_SUPPLIER_TAX,
    	TE.TAX_ID AS  ASHF_TAX_ID,
        dd.DATE_VALUE AS RECEIPT_DATE,
        rc.ENTRY_TIME,
        rc.MOST_RECENT_PROCESS_TIME,
        MAX(CASE WHEN rrte.trigger_code = '7' THEN 1 ELSE 0 END) AS has_rrte_7,
        MAX(CASE WHEN rrt.trigger_code = '1146' THEN 1 ELSE 0 END) AS has_rrt_1146,
        ROW_NUMBER() OVER (PARTITION BY rc.CLAIM_HCC_ID ORDER BY rc.MOST_RECENT_PROCESS_TIME) AS rn_exception
    FROM
        RecentClaims rc
    LEFT JOIN
        payor_dw.CLAIM_FACT_TO_EXCEPTION cfte ON rc.claim_fact_key = cfte.claim_fact_key
    LEFT JOIN
        payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
    LEFT JOIN
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.claim_fact_key = cftrt.claim_fact_key
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
    LEFT JOIN
        payor_dw.supplier ashf ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
    LEFT JOIN
        payor_dw.SUPPLIER_LOCATION sl ON rc.LOCATION_KEY = sl.SUPPLIER_LOCATION_KEY 
    LEFT JOIN
        payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
    LEFT JOIN 
		PAYOR_DW.TAX_ENTITY TE ON ashf.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY 
	WHERE 
		rc.row_num =1
		AND rc.ENTRY_TIME >= TO_TIMESTAMP('2024-09-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
		AND rc.ENTRY_TIME < TO_TIMESTAMP('2024-09-21 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    GROUP BY 
        rc.CLAIM_HCC_ID, 
        rc.EXTERNAL_CLAIM_NUMBER,
        rc.CLAIM_STATUS,
        rc.SI_SUPPLIER_NAME,
        ashf.SUPPLIER_NAME,
        rc.SI_SUPPLIER_NPI,
        ashf.SUPPLIER_NPI,
        rc.SI_SUPPLIER_ID,
        ashf.SUPPLIER_HCC_ID,
        rc.FACILITY_LOCATION_NAME,
        sl.SUPPLIER_LOCATION_NAME,
        rc.FACILITY_LOCATION_ID,
        sl.SUPPLIER_LOCATION_HCC_ID,
        rc.FACILITY_LOCATION_NPI,
        sl.SUPPLIER_LOCATION_NPI,
        rc.SI_SUPPLIER_TAX,
    	TE.TAX_ID,
        dd.DATE_VALUE,
        rc.ENTRY_TIME,
        rc.MOST_RECENT_PROCESS_TIME
)
SELECT 
    cwt.CLAIM_HCC_ID,
    cwt.EXTERNAL_CLAIM_NUMBER,
    cwt.CLAIM_STATUS AS FIRST_CLAIM_STATUS,
    CASE 
        WHEN cwt.has_rrte_7 = 1 AND cwt.has_rrt_1146 = 1 THEN 'Yes'
        WHEN cwt.has_rrte_7 = 1 THEN 'Yes'
        ELSE 'No'
    END AS SupplierNotIdentified,
    CASE 
        WHEN cwt.has_rrte_7 = 1 AND cwt.has_rrt_1146 = 1 THEN 'Yes'
        WHEN cwt.has_rrt_1146 = 1 THEN 'Yes'
        ELSE 'No'
    END AS SupplierLocationRequired,
    cwt.SI_SUPPLIER_NAME,
    cwt.ASHF_SUPPLIER_NAME,
    cwt.SI_SUPPLIER_NPI,
    cwt.ASHF_SUPPLIER_NPI,
    cwt.SI_SUPPLIER_ID,
    cwt.ASHF_SUPPLIER_HCC_ID,
    cwt.SUBMITTED_LOCATION_NAME,
    cwt.SUPPLIER_LOCATION_NAME,
    cwt.SUBMITTED_LOCATION_ID,
    cwt.SUPPLIER_LOCATION_HCC_ID,
    cwt.SUBMITTED_LOCATION_NPI,
    cwt.SUPPLIER_LOCATION_NPI,
    cwt.ASHF_TAX_ID,
    cwt.SI_SUPPLIER_TAX,
    cwt.RECEIPT_DATE,
    cwt.ENTRY_TIME,
    cwt.MOST_RECENT_PROCESS_TIME
FROM
    ClaimWithTriggers cwt
WHERE
    cwt.rn_exception = 1
    AND cwt.ASHF_TAX_ID ='95-1683892'
    AND cwt.EXTERNAL_CLAIM_NUMBER = 'A242601000400'
    
--------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------    
    
  WITH RecentClaims AS (
    SELECT
        cf.*,
        ROW_NUMBER() OVER (PARTITION BY cf.CLAIM_HCC_ID ORDER BY cf.MOST_RECENT_PROCESS_TIME ASC) as row_num
    FROM
        payor_dw.claim_fact cf
    WHERE
        cf.IS_CONVERTED = 'N' AND
        cf.IS_TRIAL_CLAIM = 'N' AND
        cf.ENTRY_TIME >= TO_TIMESTAMP('2024-07-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
ClaimTriggers AS (
    SELECT 
        rc.CLAIM_FACT_KEY,
        rc.CLAIM_HCC_ID,
        rc.CLAIM_STATUS,
        MAX(CASE WHEN rrte.trigger_code = '7' THEN 1 ELSE 0 END) AS has_exception_trigger,
        MAX(CASE WHEN rrt.trigger_code = '1146' THEN 1 ELSE 0 END) AS has_review_trigger
    FROM
        RecentClaims rc
    LEFT JOIN
        payor_dw.CLAIM_FACT_TO_EXCEPTION cfte ON rc.claim_fact_key = cfte.claim_fact_key
    LEFT JOIN
        payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
    LEFT JOIN
        payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.claim_fact_key = cftrt.claim_fact_key
    LEFT JOIN
        payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
    WHERE
        rc.row_num = 1
    GROUP BY
        rc.CLAIM_FACT_KEY, rc.CLAIM_HCC_ID, rc.CLAIM_STATUS
)
SELECT DISTINCT 
    rc.CLAIM_HCC_ID,
    rc.EXTERNAL_CLAIM_NUMBER,
    rc.CLEARING_HOUSE_TRACE_NUMBER,	
    rc.CLAIM_STATUS,
    CASE
        WHEN ct.has_exception_trigger = 1 AND ct.has_review_trigger = 1 THEN 'Yes'
        WHEN ct.has_exception_trigger = 1 THEN 'Yes'
        WHEN ct.has_review_trigger = 1 THEN 'No'
        ELSE 'No'
    END AS Supplier_Not_Identified,
    CASE
        WHEN ct.has_exception_trigger = 1 AND ct.has_review_trigger = 1 THEN 'Yes'
        WHEN ct.has_review_trigger = 1 THEN 'Yes'
        WHEN ct.has_exception_trigger = 1 THEN 'No'
        ELSE 'No'
    END AS Supplier_Location_Required,
    rc.SI_SUPPLIER_NAME,
    ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
    rc.SI_SUPPLIER_ID,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    rc.SI_SUPPLIER_NPI,
    ashf.SUPPLIER_NPI AS ASHF_SUPPLIER_NPI,
    rc.SI_SUPPLIER_TAX,
    TEHF.TAX_ID AS ASHF_TAX_ID,
    rc.FACILITY_LOCATION_NAME AS SUBMITTED_LOCATION_NAME,
    sl.SUPPLIER_LOCATION_NAME,
    rc.FACILITY_LOCATION_ID AS SUBMITTED_LOCATION_ID,
    sl.SUPPLIER_LOCATION_HCC_ID,
    rc.FACILITY_LOCATION_NPI AS SUBMITTED_LOCATION_NPI,
    sl.SUPPLIER_LOCATION_NPI,
    dd.DATE_VALUE AS RECEIPT_DATE,
    rc.ENTRY_TIME,
    rc.MOST_RECENT_PROCESS_TIME
FROM
    RecentClaims rc
LEFT JOIN
    ClaimTriggers ct ON rc.CLAIM_FACT_KEY = ct.CLAIM_FACT_KEY
LEFT JOIN
    payor_dw.CLAIM_SOURCE_CODE csc ON rc.CLAIM_SOURCE_KEY = csc.CLAIM_SOURCE_KEY
LEFT JOIN
    payor_dw.CLAIM_FACT_TO_EXCEPTION cfte ON rc.claim_fact_key = cfte.claim_fact_key
LEFT JOIN
    payor_dw.review_repair_trigger rrte ON cfte.review_repair_trigger_key = rrte.review_repair_trigger_key
LEFT JOIN
    payor_dw.CLAIM_FACT_TO_REVIEW_TRIGGER cftrt ON rc.claim_fact_key = cftrt.claim_fact_key
LEFT JOIN
    payor_dw.review_repair_trigger rrt ON cftrt.review_repair_trigger_key = rrt.review_repair_trigger_key
LEFT JOIN
    payor_dw.DATE_DIMENSION dd ON rc.RECEIPT_DATE_KEY = dd.DATE_KEY
LEFT JOIN
	payor_dw.supplier ashf ON rc.SUPPLIER_KEY = ashf.SUPPLIER_KEY
LEFT JOIN
	PAYOR_DW.TAX_ENTITY TEHF ON ashf.TAX_ENTITY_KEY = TEHF.TAX_ENTITY_KEY
LEFT JOIN
	PAYOR_DW.SUPPLIER_LOCATION sl ON ashf.SUPPLIER_KEY = sl.SUPPLIER_KEY
WHERE
	rc.row_num = 1
	AND rc.ENTRY_TIME >= TO_TIMESTAMP('2024-09-19 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    AND rc.ENTRY_TIME < TO_TIMESTAMP('2024-09-21 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    AND TEHF.TAX_ID = '95-1683892'