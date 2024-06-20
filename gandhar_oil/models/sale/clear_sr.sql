/* {{ source('sap', 'sales') }} */
/* {{ ref('sr_document_type') }} */
/*{tableName: "outward_sales_register_clrtax"}*/
/*{seedfile'{"sr_document_type.csv": ref'(Document Type)}}*/
/*{CTE: "EXPR_TYPE: ref'(Document Number)"}*/

--basic document details
SELECT
-- DT.Document_type AS "Document Type",
CASE 
    WHEN CT.TRANS_TYPE = 'INV' AND CT.INVH_TXN_CODE IN ('DSASIL','DSATAL','DSA-V') THEN 'Debit Note'
    WHEN CT.TRANS_TYPE = 'INV' THEN 'Invoice'
    WHEN CT.TRANS_TYPE = 'SARTN' THEN 'Credit Note'
    ELSE ''
END AS "Document Type",

--below syntax we concatenate both "INVH_TXN_CODE" & "INVH_NO" as per client request. combine result column length is 16 or below 16 charachter.
CT.INVH_TXN_CODE || CT.INVH_NO  AS "Document Number",
CT.INVH_DT AS "Document Date",

-- TO_CHAR(INVH_DT, 'MM') AS "Return Filing Month"
-- LEFT(CT.GST_NO, 2) AS "Place of Supply",
CASE
    WHEN CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') THEN '96'
    WHEN CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND CT.GST_NO IN (' ',NULL,'URP','*URP*','0','-') THEN '96'
    WHEN CT.GST_NO IN ('URP','*URP*') AND CT.INVH_FORM_CODE = 'LOCAL' THEN ' '
    ELSE LEFT(CT.GST_NO,2)
END AS "Place of Supply",

null "Applicable Tax Rate",

-- we collect all the details of Bill of supply from INVH_FORM_CODE column as per client
CASE
    when CT.INVH_FORM_CODE IN ('EXEMPT','NIL RATED','NON-GST') THEN 'Y'
    ELSE NULL
END AS 'Is this a Bill of supply',
    
null "Is Reverse Charge",
null "Is GST TDS Deducted",

-- supplier details
CT.GST_NOO AS "My GSTIN",
-- null "Ecommerce GSTIN",
CT.COMP_NAME AS "Company Name",
CT.INVH_COMP_CODE AS "Comapany Code",

-- customer details
-- CT.GST_NO AS "Customer GSTIN",
CASE    
    WHEN CT.INVH_FORM_CODE = 'LOCAL' AND CT.GST_NO IN ('URP','URP*') THEN ' '
    WHEN CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN')  THEN 'URP'
    ELSE CT.GST_NO
END AS "Customer GSTIN",
CT.CUST_NAME AS "Customer Name",
COALESCE(CT.ADDRESS, NULL) AS "Customer Address",
COALESCE(CT.CITY_CODE, NULL) AS "Customer City",
-- COALESCE(CT.STATE_CODE, NULL) AS "Customer State",
null "Customer State",
null "Customer Taxpayer Type",


-- item details
CASE
    WHEN LEFT(CT.HSN_CODE,2)= 99 THEN 'S'
    WHEN CT.HSN_CODE IS NOT NULL THEN 'G'
    ELSE NULL
END AS "Item_Category",
CT.INVI_ITEM_DESC AS "Item Description",
CT.HSN_CODE AS "HSN or SAC code",
CT.DIS_QTY AS "Item Quantity",
CT.BASE_UOM AS "Item Unit Code",
CT.INR_RATE AS "Item Unit Price",
CT.DIS AS "Item Discount Amount",
CT.ITEM_TOTAL AS "Item Taxable Amount",

--zero tax::number "Zero Tax Category",
CASE
    WHEN CT.INVH_FORM_CODE = 'EXEMPTED' THEN 'EXEMPTED'
    WHEN CT.INVH_FORM_CODE = 'NIL_RATED' THEN 'NIL_RATED'
    WHEN CT.INVH_FORM_CODE = 'NON_GST_SUPPLY' THEN 'NON_GST_SUPPLY'
    ELSE NULL
END AS "Zero Tax Category",


--tax_rate & tax_amount 
COALESCE(CT.GST_RTE, 0) AS "GST Rate",
-- ROUND((CT.CGST_TOTAL / CT.ITEM_TOTAL) * 100, 2) AS "CGST Rate",
(CASE WHEN (CT.CGST_TOTAL != 0 OR CT.CGST_TOTAL IS NOT NULL) THEN CT.CGST_TOTAL ELSE 0 END) AS "CGST Amount",

-- ROUND(((CT.SGUST_TOTAL) / CT.ITEM_TOTAL) * 100, 2) AS "SGST Rate",
(CASE WHEN (CT.SGUST_TOTAL != 0 OR CT.SGUST_TOTAL IS NOT NULL) THEN CT.SGUST_TOTAL ELSE 0 END) AS "SGST Amount",

-- ROUND((CT.IGST_TOTAL / CT.ITEM_TOTAL) * 100, 2) AS "IGST Rate",
(CASE WHEN (CT.IGST_TOTAL != 0 OR CT.IGST_TOTAL IS NOT NULL) THEN CT.IGST_TOTAL ELSE 0 END) AS "IGST Amount",

-- ROUND((CT.CESS / CT.ITEM_TOTAL) * 100, 2) AS "CESS Rate",
CT.CESS AS "CESS Amount",

CT.TCS_TOTAL AS "TCS",
-- CT.TOTAL AS "Document Total Amount",
SUM(CT.TOTAL) OVER(PARTITION BY CT.INVH_NO,CT.INVH_TXN_CODE) AS "Document Total Amount",

-- export details
CASE 
    WHEN CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND CT.GST_RTE <> 0 THEN 'EXPWP'
    WHEN CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND (CT.GST_RTE = 0 OR CT.GST_RTE IS NULL OR CT.GST_RTE = ' ' ) THEN 'EXPWOP'
    WHEN CT.INVH_FORM_CODE = 'SEZ SALE' AND CT.GST_RTE <> 0 THEN 'SEZWP'
    WHEN CT.INVH_FORM_CODE = 'SEZ SALE' AND (CT.GST_RTE = 0 OR CT.GST_RTE IS NULL OR CT.GST_RTE = ' ' ) THEN 'SEZWOP'
    ELSE NULL
END AS "Export Type",
-- CT.SHIPPING_BILL_NO AS "Export Bill Number",
-- CASE
--     WHEN CT.SHIPPING_BILL_NO IN ('1H','0','J') THEN ''
--     ELSE CT.SHIPPING_BILL_NO
-- END AS "Export Bill Number",
CASE
    WHEN LEFT(CT.HSN_CODE,2) != 99 AND CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND CT.GST_RTE <> 0 THEN CT.SHIPPING_BILL_NO
    WHEN LEFT(CT.HSN_CODE,2) != 99 AND CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND CT.GST_RTE = 0 THEN CT.SHIPPING_BILL_NO
    ELSE ' '
END AS "Export Bill Number",
-- CT.SHIPPING_BILL_DT AS "SHIPPING_BILL_DT",
CASE
    WHEN LEFT(CT.HSN_CODE,2) != 99 AND CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND CT.GST_RTE <> 0 THEN CT.SHIPPING_BILL_DT
    WHEN LEFT(CT.HSN_CODE,2) != 99 AND CT.INVH_FORM_CODE IN ('EXPORT','NEPAL','BHUTAN') AND CT.GST_RTE = 0 THEN CT.SHIPPING_BILL_DT
    ELSE ' '
END AS "Export Bill Date",
CT.EXPORT_CODE AS "Export Port Code",
-- null "Export Bill Number",
-- null "Export Bill Date",
-- null "Export Port Code",


-- CASE    
--     when EXP.export_type in ('SEZWP','SEZWOP') and EXP.item_type ='G' then "GOODS FROM SEZ"
--     when EXP.export_type in ('SEZWP','SEZWOP') and EXP.item_type ='S' then "SERVICE FROM SEZ"
--     when EXP.export_type in ('EXPWP','EXPWOP') and EXP.item_type ='S' then "SERVICE FROM SEZ"
--     when EXP.export_type in ('EXPWP','EXPWOP') and EXP.item_type ='G' then "GOODS FROM SEZ"
--     ELSE NULL
-- END AS "Import Type",


-- voucher details
--voucher_typ::varchar "Voucher Type",
--voucher_no::varchar "Voucher Number",
--null "Voucher Date",
--is_document_cancelled::varchar "Is this Document Cancelled",

-- advance adjustment details
null "Linked Advance Document Number",
null "Linked Advance Document Date",
null "Linked Advance Adjustment Amount",

-- in case of admendments
null "Original Document Number",
null "Original Document Date",
null "Original Document Customer GSTIN",

-- in case of cdns
null "Linked Invoice Number",
null "Linked Invoice Date",
null "Linked Invoice Customer GSTIN",
null "Is Linked Invoice Pre GST",
null "Reason for Issuing CDN"

--posnr::varchar "External Line Item ID"


-- table_name 
from raw.sales AS CT
WHERE CT.INVH_TXN_CODE NOT IN ('EINVC-TAL-BC','EINVC-TAL','EINVC-SIL','DNCNGS')
-- LEFT OUTER JOIN s.sr_document_type AS DT ON CT.INVH_TXN_CODE = DT.INVH_TXN_CODE
-- LEFT OUTER JOIN total_document 
--     ON total_document.INVH_NO = CT.INVH_NO AND total_document.INVH_TXN_CODE = CT.INVH_TXN_CODE  
-- LEFT OUTER JOIN exp_type AS EXP 
--     ON CT.INVH_NO = EXP.invh_no and CT.INVH_TXN_CODE = EXP.code