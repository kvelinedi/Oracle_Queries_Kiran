    
 SELECT CVC_STEP_ID, CVC_ID ,CVC_NAME,COORDINATOR, EVENT_TRANSACTION_STATE_CODE, HCC_ID ,FULL_NAME ,LAST_UPDATE_TIME ,LAST_DW_UPDATE_TIME ,OPS_NAME,
 TRANSACTION_START_TIME , TRANSACTION_END_TIME ,TRANSACTION_STATE_CODE ,TRANSACTION_TYPE , type_name, FAILURE_TXT ,CLAIM_FACT_KEY 
 FROM 
 	TRANSACTION_STEP_FACT tsf 
 WHERE 
     tsf.TRANSACTION_START_TIME >= TO_TIMESTAMP('2024-09-17 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND 
     tsf.TRANSACTION_END_TIME < TO_TIMESTAMP('2024-09-21 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 