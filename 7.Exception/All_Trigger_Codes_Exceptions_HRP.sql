    
SELECT DISTINCT
    rrt.trigger_code AS TRIGGER_CODE,
    rrt.trigger_desc AS TRIGGER_DESC
--    rrt.TRIGGER_DOMAIN_NAME
--    rrt.POLICY_NAME     
FROM
    payor_dw.review_repair_trigger rrt
WHERE
    rrt.trigger_code IS NOT NULL
    AND rrt.IS_EXPIRED  = 'U'
    AND rrt.IS_APPROVED = 'U'
    --AND rrt.trigger_code IN ('7')
ORDER BY
    rrt.trigger_code;