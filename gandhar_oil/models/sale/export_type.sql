/* {{ source('sap', 'sales') }} */
select 
    INVH_NO AS "invh_no",
    CASE
        WHEN LEFT(HSN_CODE,2) = 99 THEN 'S'
        WHEN HSN_CODE IS NOT NULL THEN 'G'
        ELSE NULL
    END AS "item_category",
    -- CASE
    --     WHEN REGEXP_LIKE(HSN_CODE, '^0*99.*') THEN 'S'
    --     WHEN HSN_CODE IS NOT NULL THEN 'G'
    --     ELSE NULL
    -- END AS "Item Category",
    CASE 
        WHEN INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND GST_RTE <> 0 THEN 'EXPWP'
        WHEN INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND GST_RTE = 0 THEN 'EXPWOP'
        WHEN INVH_FORM_CODE = 'SEZ SALE' AND GST_RTE <> 0 THEN 'SEZWP'
        WHEN INVH_FORM_CODE = 'SEZ SALE' AND GST_RTE = 0 THEN 'SEZWOP'
        ELSE NULL
    END AS "export_type"
from 
    raw.sales 
    
