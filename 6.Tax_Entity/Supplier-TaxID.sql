SELECT DISTINCT
	TE.TAX_ID,
	s.SUPPLIER_HCC_ID
FROM
	payor_dw.SUPPLIER s
LEFT JOIN
	PAYOR_DW.TAX_ENTITY TE ON s.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY
WHERE
    TE.TAX_ID IS NOT NULL
ORDER BY
    TE.TAX_ID;
-------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------
 SELECT 
    TE.TAX_ID AS ASHF_TAX_ID,
    ashf.SUPPLIER_HCC_ID AS ASHF_SUPPLIER_HCC_ID,
    ashf.SUPPLIER_NPI  AS ASHF_SUPPLIER_NPI,
    ashf.SUPPLIER_NAME AS ASHF_SUPPLIER_NAME,
    pt.PROVIDER_TAXONOMY_NAME ,
    sl.SUPPLIER_LOCATION_HCC_ID ,
    sl.SUPPLIER_LOCATION_NAME ,
    sl.SUPPLIER_LOCATION_NPI ,
    PA.ADDRESS_LINE AS ASHF_SUPPLIER_ADDRESS,
    PA.CITY_NAME AS ASHF_SUPPLIER_CITY,
    PA.STATE_CODE AS ASHF_SUPPLIER_STATE ,
    PA.ZIP_CODE AS ASHF_SUPPLIER_ZIPCODE
FROM
payor_dw.supplier ashf
LEFT JOIN
	payor_dw.POSTAL_ADDRESS pa ON ASHF.SUPPLIER_CORR_ADDRESS_KEY = pa.POSTAL_ADDRESS_KEY 
LEFT JOIN 
	payor_dw.SUPPLIER_LOCATION sl ON ashf.SUPPLIER_KEY = sl.SUPPLIER_KEY 
LEFT JOIN 
	payor_dw.TAX_ENTITY te ON ASHF.TAX_ENTITY_KEY = TE.TAX_ENTITY_KEY 
LEFT JOIN
	payor_dw.PROVIDER_TAXONOMY pt ON ashf.PRIMARY_CLASSIFICATION_KEY = pt.PROVIDER_TAXONOMY_KEY
WHERE 
 TE.TAX_ID = '35-2407578'