   SELECT 
    dd3.DATE_VALUE AS SUPPLIER_LOC_TERM_DATE,
    dd2.DATE_VALUE AS SUPPLIER_LOC_EFF_DATE,
    dd1.DATE_VALUE AS SUPPLIER_TERMINATION_DATE,
    dd.DATE_VALUE AS SUPPLIER_EFFECTIVE_DATE,
--    ashf.SUPPLIER_STATUS,
    hfsc.HISTORY_FACT_STATUS_DESC AS SUPPLIER_STATUS,
--    sl.SUPPLIER_LOCATION_STATUS,
    hfsc1.HISTORY_FACT_STATUS_DESC ,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_ID,
    sl.SUPPLIER_LOCATION_HCC_ID AS SUPPLIER_LOCATION_ID,
    sl.SUPPLIER_LOCATION_NAME,
	ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
	ashf.SUPPLIER_CONTACT_TITLE,
	ashf.SUPPLIER_NPI  AS ASHF_SUPPLIER_NPI,
    sl.SUPPLIER_LOCATION_NPI,
    pt.CLASSIFICATION AS Supplier_Classification
--    TE.TAX_ID AS  ASHF_TAX_ID,
FROM 
	payor_dw.supplier ashf 
--	payor_dw.ALL_SUPPLIER_HISTORY_FACT ashf 
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
WHERE 
	ashf.SUPPLIER_HCC_ID = '9001170'

-------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------

SELECT 
    dd3.DATE_VALUE AS SUPPLIER_LOC_TERM_DATE,
    dd2.DATE_VALUE AS SUPPLIER_LOC_EFF_DATE,
    dd1.DATE_VALUE AS SUPPLIER_TERMINATION_DATE,
    dd.DATE_VALUE AS SUPPLIER_EFFECTIVE_DATE,
--    ashf.SUPPLIER_STATUS,
    hfsc.HISTORY_FACT_STATUS_DESC AS SUPPLIER_STATUS,
--    sl.SUPPLIER_LOCATION_STATUS,
    hfsc1.HISTORY_FACT_STATUS_DESC ,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_ID,
    sl.SUPPLIER_LOCATION_HCC_ID AS SUPPLIER_LOCATION_ID,
    sl.SUPPLIER_LOCATION_NAME,
	ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
	ashf.SUPPLIER_CONTACT_TITLE,
	ashf.SUPPLIER_NPI  AS ASHF_SUPPLIER_NPI,
    sl.SUPPLIER_LOCATION_NPI,
    pt.CLASSIFICATION AS Supplier_Classification
--    TE.TAX_ID AS  ASHF_TAX_ID,
FROM 
	payor_dw.supplier ashf 
--	payor_dw.ALL_SUPPLIER_HISTORY_FACT ashf 
JOIN 
	payor_dw.DATE_DIMENSION dd ON ASHF.FIRST_EFFECTIVE_DATE_KEY = dd.date_key
JOIN
	payor_dw.DATE_DIMENSION dd1 ON ASHF.SUPPLIER_TERMINATION_DATE_KEY = dd1.date_key
LEFT JOIN
	payor_dw.POSTAL_ADDRESS pa ON ASHF.SUPPLIER_CORR_ADDRESS_KEY = pa.POSTAL_ADDRESS_KEY
LEFT JOIN 
	payor_dw.SUPPLIER_LOCATION sl ON ashf.SUPPLIER_KEY = sl.SUPPLIER_KEY 
JOIN
	payor_dw.DATE_DIMENSION dd2 ON sl.SUPPLIER_LOC_EFF_DATE_KEY = dd2.date_key
JOIN
	payor_dw.DATE_DIMENSION dd3 ON sl.SUPPLIER_LOC_TERM_DATE_KEY = dd3.date_key
LEFT JOIN 
	PAYOR_DW.TAX_ENTITY TE ON ashf.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY 
LEFT JOIN
	payor_dw.PROVIDER_TAXONOMY pt ON ashf.PRIMARY_CLASSIFICATION_KEY = pt.PROVIDER_TAXONOMY_KEY
LEFT JOIN 
    payor_dw.HISTORY_FACT_STATUS_CODE hfsc ON ashf.SUPPLIER_STATUS = hfsc.HISTORY_FACT_STATUS_CODE 
LEFT JOIN 
    payor_dw.HISTORY_FACT_STATUS_CODE hfsc1 ON sl.SUPPLIER_LOCATION_STATUS = hfsc1.HISTORY_FACT_STATUS_CODE 
WHERE 
	ashf.SUPPLIER_HCC_ID IN ('9003728','S0000115','1013602','9900411','9900414','9900420','9900413','9900415','9900492','9903548','9001170','9900410','9900432','1004839','9900409','32471','32825','34652','36002','1000020','1000021','1000021','1000187','1000193','1000200','1000201','1000202','1000203','1000204','1000205','1000206','1001169','1001170','1001531','1002039','1002041','1002047','1002053','1012339','1012341','1098232','S0000699')
