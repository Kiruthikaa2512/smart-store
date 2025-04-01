SELECT 'products' AS TableName, 
       product_id AS productid, 
       product_name AS productname, 
       category AS category, 
       unit_price AS unitprice, 
       days_to_receive AS stockquantity, 
       customizable AS supplier, 
       NULL AS storeid, 
       NULL AS campaignid, 
       NULL AS saleamount, 
       NULL AS bonuspoints, 
       NULL AS paymenttype
FROM products
UNION ALL
SELECT 'sales' AS TableName, 
       NULL AS productid, 
       NULL AS productname, 
       NULL AS category, 
       NULL AS unitprice, 
       NULL AS stockquantity, 
       NULL AS supplier, 
       store_id AS storeid, 
       campaign_id AS campaignid, 
       sales_amount AS saleamount, 
       quantity AS bonuspoints, 
       bill_type AS paymenttype
FROM sales;