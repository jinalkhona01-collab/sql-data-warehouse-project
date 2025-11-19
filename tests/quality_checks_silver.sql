/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================


---Check for Nulls or Duplicates in primary key
--- Expectations: No result

---cust_info table
SELECT  cst_id , COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR  cst_id IS NULL;

---prd_info table
SELECT  prd_id , COUNT(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR  prd_id IS NULL;

---sales_details table
--Check for unwanted spaces
--- Expectations: No result

---cust_info table
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE  cst_firstname != TRIM(cst_firstname);

---prd_info table
SELECT prd_nm
FROM silver.crm_prd_info
WHERE  prd_nm != TRIM(prd_nm);

---sales_details
SELECT
sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

---Data Standarization & Consistency

---cust_info table
SELECT  DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT  DISTINCT cst_marital_status
FROM silver.crm_cust_info;

---prd_info table
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--silver.erp_cust_az12 table
SELECT DISTINCT gen
FROM silver.erp_cust_az12 

---silver.erp_loc_a101 table
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

---Check for Nulls or Negatice numbers
--- Expectations: No result

---prd_info table
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

---Check for Invalid Date Orders
--- Expectations: No result

---prd_info table
SELECT *
FROM  silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
--test query--
SELECT
prd_id,
prd_key,
prd_nm,
prd_line,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS prd_end_dt_test
FROM silver.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R' , 'AC-HE-HL-U509' );

---sales_details table
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt >sls_due_dt;

---Check for Invalid Dates--
---sales_details table
SELECT 
NULLIF (sls_order_dt,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LENGTH(sls_order_dt::text) != 8 
OR sls_order_dt > 202500101 
OR sls_order_dt < 19000101;

SELECT 
NULLIF (sls_ship_dt,0) AS sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LENGTH(sls_ship_dt::text) != 8 
OR sls_ship_dt > 202500101 
OR sls_ship_dt < 19000101;

SELECT 
NULLIF (sls_due_dt,0) AS sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
OR LENGTH(sls_due_dt::text) != 8 
OR sls_due_dt > 202500101 
OR sls_due_dt < 19000101;

---Check Data consistency: Between Sales, Qty and price
---Sales = Qty * Proce
--- Values must not be NULL, zero or negative

--Sales_details table
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR  sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
     THEN sls_sales / NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price
FROM silver.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price) 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price;


---Identify Out of Range Dates
--silver.erp_cust_az12 table
 SELECT DISTINCT
 bdate
 FROM silver.erp_cust_az12
 WHERE bdate > CURRENT_DATE



SELECT * 
FROM silver.crm_cust_info;

SELECT * 
FROM silver.crm_prd_info;


SELECT * 
FROM silver.crm_sales_details;

SELECT * 
FROM silver.erp_cust_az12;

SELECT * 
FROM silver.erp_loc_a101;

SELECT * 
FROM silver.erp_px_cat_g1v2;