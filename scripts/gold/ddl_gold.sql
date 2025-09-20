/*
=======================================================================================
DDL Script: Create Gold Views
=======================================================================================
Script Purpose:
  This script creates views for the Gold layer in the data warehouse. 
  The Gold layer represents the final dimension and fact tables (Star Schema)

Each view performs transformations and combines data from the Silver layer 
to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
=======================================================================================
*/
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
GO
  
CREATE VIEW gold.dim_customers AS (
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender Info
		 ELSE COALESCE(ca.GEN, 'n/a')
	END AS gender,
	ca.BDATE as birth_date,    
	ci.cst_create_date as create_date  -- TIP After Joining table, check if any duplicate where introduced by the join logic

FROM silver.crm_cust_info as ci 
LEFT JOIN silver.erp_CUST_AZ12 ca
ON      ci.cst_key=ca.cid
LEFT JOIN silver.erp_LOC_A101 la
ON      ci.cst_key=la.CID);

/*============================================================================
-- Create dim_products
==============================================================================
*/IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS (
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_num as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as sub_category,
	pc.MAINTENANCE as maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
	
FROM silver.crm_prd_info as pn
LEFT JOIN silver.erp_PX_CAT_G1V2 as pc
ON    pn.cat_id = pc.id 
WHERE prd_end_dt IS NULL)     -- Filter out all historical data

/*============================================================================
-- Create Fact Sales
==============================================================================
*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
  
CREATE VIEW gold.fact_sales as(
SELECT 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr 
ON    sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON    sd.sls_cust_id = cu.customer_id)
