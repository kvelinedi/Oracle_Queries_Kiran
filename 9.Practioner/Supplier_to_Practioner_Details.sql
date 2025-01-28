SELECT 
	TE.TAX_ID AS  ASHF_TAX_ID,
--    dd2.DATE_VALUE AS SUPPLIER_LOC_EFF_DATE,
--    dd.DATE_VALUE AS SUPPLIER_EFFECTIVE_DATE,
--    ashf.SUPPLIER_STATUS,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_ID,
    sl.SUPPLIER_LOCATION_HCC_ID AS SUPPLIER_LOCATION_ID,
    hfsc.HISTORY_FACT_STATUS_DESC AS SUPPLIER_STATUS,
--    sl.SUPPLIER_LOCATION_STATUS ,
    hfsc1.HISTORY_FACT_STATUS_DESC AS SUPPLIER_LOCATION_STATUS,
    dd1.DATE_VALUE AS SUPPLIER_TERMINATION_DATE,
    dd3.DATE_VALUE AS SUPPLIER_LOC_TERM_DATE,
    sl.SUPPLIER_LOCATION_NAME,
	ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
--	ashf.SUPPLIER_CONTACT_TITLE,
	ashf.SUPPLIER_NPI  AS ASHF_SUPPLIER_NPI,
    sl.SUPPLIER_LOCATION_NPI,
    pt.CLASSIFICATION AS Supplier_Classification,
    p.PRACTITIONER_HCC_ID,
    p.PRACTITIONER_FULL_NAME,
    p.PRACTITIONER_NPI,
--    p.PRACTITIONER_STATUS,
    hfsc2.HISTORY_FACT_STATUS_DESC AS PRACTITIONER_STATUS,
    prhf.PRACTITIONER_ROLE_NAME,
--    alef.ACTION_TYPE_CODE ,
    atc.ACTION_TYPE_DESC ,
    alef.ENTRY_TIME ,
    alef.LOG_NOTE ,
    alef.MESSAGE_DESC ,
--    alef.HCC_USER_ID ,
    ua.USER_NAME 
FROM 
	payor_dw.supplier ashf
LEFT JOIN 
	payor_dw.DATE_DIMENSION dd ON ASHF.FIRST_EFFECTIVE_DATE_KEY = dd.date_key
LEFT JOIN
	payor_dw.DATE_DIMENSION dd1 ON ASHF.SUPPLIER_TERMINATION_DATE_KEY = dd1.date_key
LEFT JOIN
	payor_dw.POSTAL_ADDRESS pa ON ASHF.SUPPLIER_CORR_ADDRESS_KEY = pa.POSTAL_ADDRESS_KEY
LEFT JOIN 
	payor_dw.SUPPLIER_LOCATION sl ON ashf.SUPPLIER_KEY = sl.SUPPLIER_KEY 
LEFT JOIN
	payor_dw.DATE_DIMENSION dd2 ON sl.SUPPLIER_LOC_EFF_DATE_KEY = dd2.date_key
LEFT JOIN
	payor_dw.DATE_DIMENSION dd3 ON sl.SUPPLIER_LOC_TERM_DATE_KEY = dd3.date_key
LEFT JOIN 
	PAYOR_DW.TAX_ENTITY TE ON ashf.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY 
LEFT JOIN
	payor_dw.PROVIDER_TAXONOMY pt ON ashf.PRIMARY_CLASSIFICATION_KEY = pt.PROVIDER_TAXONOMY_KEY
LEFT JOIN 
    payor_dw.HISTORY_FACT_STATUS_CODE hfsc ON ashf.SUPPLIER_STATUS = hfsc.HISTORY_FACT_STATUS_CODE 
LEFT JOIN 
    payor_dw.HISTORY_FACT_STATUS_CODE hfsc1 ON sl.SUPPLIER_LOCATION_STATUS = hfsc1.HISTORY_FACT_STATUS_CODE 
LEFT JOIN 
	payor_dw.PRACTITIONER_ROLE_HISTORY_FACT prhf ON sl.SUPPLIER_LOCATION_KEY = prhf.SUPPLIER_LOCATION_KEY 
LEFT JOIN 
	payor_dw.PRACTITIONER p ON prhf.PRACTITIONER_KEY = p.PRACTITIONER_KEY
LEFT JOIN 
    payor_dw.HISTORY_FACT_STATUS_CODE hfsc2 ON p.PRACTITIONER_STATUS = hfsc2.HISTORY_FACT_STATUS_CODE 
LEFT JOIN 
	payor_dw.DATE_DIMENSION effDt ON prhf.VERSION_EFF_DATE_KEY = effDt.DATE_KEY 
LEFT JOIN 
    payor_dw.DATE_DIMENSION expDt ON prhf.VERSION_EXP_DATE_KEY = expDt.DATE_KEY
LEFT JOIN 
	payor_dw.AUDIT_LOG_ENTRY_FACT alef ON ashf.AUDIT_LOG_KEY = alef.AUDIT_LOG_KEY 
LEFT JOIN 
    payor_dw.ACTION_TYPE_CODE atc ON alef.ACTION_TYPE_CODE = atc.ACTION_TYPE_CODE 
LEFT JOIN 
	payor_dw.USER_ACCOUNT ua ON alef.HCC_USER_ID = ua.USER_ACCOUNT_KEY 
WHERE 
	TE.TAX_ID = '95-1683892'
	--ashf.SUPPLIER_HCC_ID = '1013602'
	AND effDt.DATE_VALUE <= SYSDATE 
    and expDt.DATE_VALUE > SYSDATE
    