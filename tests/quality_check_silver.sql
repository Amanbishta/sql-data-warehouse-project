/*
==============================================================================
Quality Checks
==============================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schema. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Invalid date ranges and orders.
    - Data consistacy between related fields.
    - Data standardization and consistency.

Usage Notes:
   - Run these checks after data loading Silver Layer.
   - Investigate and resolve any discrepancies found during the checks.
=============================================================================
*/

-- check for nulls or duplicates in primary key
-- expectation: No Result

SELECT * FROM bronze.crm_cust_info;

-- find there is duplicates in primary key or not 
select cst_id, count(*) from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;

select * from bronze.crm_cust_info
where cst_id = 29466

 -- Removing Duplicate value from Primary Key
select * from(
select *, row_number() over(PARTITION BY cst_id order by cst_create_data desc)
as flag_last
from bronze.crm_cust_info) t
where flag_last = 1

-- Check unwanted spaces in columns
-- Expectation : No Results
SELECT cst_gndr FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-- Removing unwanted spaces 
SELECT cst_id,cst_key,TRIM(cst_firstname) AS cst_firstname,TRIM(cst_lastname) as cst_lastname,
cst_marital_status,cst_gndr, cst_create_data FROM 
(select *, row_number() over(PARTITION BY cst_id order by cst_create_data desc)
as flag_last
from bronze.crm_cust_info
where cst_id IS NOT NULL) t
where flag_last = 1;

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

select * from bronze.crm_cust_info;

-- =====================================================================================
-- Clean & Load_crm_prd_info
-- =====================================================================================

SELECT * FROM bronze.crm_prd_info;

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- SUBSTRING() function use to get sub-part of an value
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Tell the end
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;

-- Check for null or duplicates values in primary key
-- Expectation: NO Result
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL; -- All Good

-- Check for NULLs or Negative Numbers
-- Exectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for Invalid Data Orders

SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt -- It is wrong format

SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

/*
==================================================================================
Clean & Load crm_sales_details
==================================================================================
*/
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_detals;

-- Check for Invalid Dates
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity*ABS(sls_price)
	  THEN sls_quantity*ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_quantity/NULLIF(sls_sales, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0


/*
============================================================================
Clean & Load erp_cust_az12
============================================================================
*/
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	 ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_CUST_AZ12;
 -- Bearth date
SELECT DISTINCT
bdate FROM bronze.erp_CUST_AZ12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Gender

SELECT DISTINCT 
gen,
CASE WHEN gen = 'M' THEN 'Male'
	WHEN gen = 'F' THEN 'Female'
	WHEN gen = ' ' THEN NULL
	ELSE gen
END AS GEN
FROM bronze.erp_CUST_AZ12;

/*
=========================================================================
Clean & Load erp_loc_a101
=========================================================================
*/
select * from bronze.erp_LOC_A101;
select cst_key from silver.crm_cust_info;

SELECT REPLACE(CID, '-',''), CNTRY FROM 
bronze.erp_LOC_A101 

-- Data Standardization & Consistency
SELECT REPLACE(CID, '-',''), CNTRY,
CASE WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
	 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN  'N/A'
	 ELSE TRIM(CNTRY)
END CNTRY
FROM bronze.erp_LOC_A101
/*
==========================================================================
Clean & Load erp+px_cat_g1v2
==========================================================================
*/
SELECT
ID,
CAT,
SUBCAT,
MAINTENANCE
FROM bronze.erp_PX_CAT_G1V2
WHERE ID NOT IN (SELECT cat_id FROM silver.crm_prd_info);

 -- Check unwanted spaces
SELECT * FROM bronze.erp_PX_CAT_G1V2
where cat != TRIM(cat) OR subcat != TRIM(subcat) or MAINTENANCE != TRIM(MAINTENANCE)

-- Data Standardization & Consistency
SELECT DISTINCT 
MAINTENANCE
FROM bronze.erp_PX_CAT_G1V2
