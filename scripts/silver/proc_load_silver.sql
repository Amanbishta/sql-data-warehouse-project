/*
=============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) process to
  populete the 'silver' schema table from the 'bronze' schema.
  Action Performed:
  - Trancates Silver tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None,
  This stord procedure dose not accept any parameters or return any values.

Usage Example:
  EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '========================================================';
		PRINT 'Loading Silver Layer';
		PRINT '========================================================';

		PRINT '--------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_data
		FROM(
			SELECT 
				*,
				ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_last
			FROM bronze.crm_cust_info where cst_id IS NOT NULL) t
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> --------------------';

		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_num,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		 )
		 SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- SUBSTRING() function use to get sub-part of an value
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Tell the end
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost, -- Replace NULL VALUE to any value
			CASE UPPER(TRIM(prd_line))
				WHEN   'M' THEN 'Mountain'
				WHEN   'R' THEN 'Road'
				WHEN   'S' THEN 'Other Sales'
				WHEN   'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line, -- Map product line codes to descriptive values
			prd_start_dt,
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> --------------------';


        -- Loading silver.crm_sales_details
		SET @start_time = GETDATE() 
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity*ABS(sls_price)
				  THEN sls_quantity*ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_quantity/NULLIF(sls_sales, 0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> --------------------';


		PRINT '--------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------';
		-- Loading silver.erp_CUST_AZ12
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_CUST_AZ12';
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		PRINT '>> Inserting Data: silver.erp_CUST_AZ12';
		INSERT INTO silver.erp_CUST_AZ12(
			cid,
			bdate,
			gen
			)
			SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Remove 'NAS' prefix if present
				 ELSE cid
			END cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END AS bdate,      -- set future birthdate to NULL
			CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				ELSE 'N/A'
			END AS gen  -- Normalize gender values and handle unkown cases
		FROM bronze.erp_CUST_AZ12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> --------------------';


		-- Loading silver.erp_LOC_A101
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_LOC_A101';
		TRUNCATE TABLE silver.erp_LOC_A101;
		PRINT '>> Inserting Data: silver.erp_LOC_A101';
		INSERT INTO silver.erp_LOC_A101(
			CID,
			CNTRY
			)
			SELECT REPLACE(CID, '-',''),
			CASE WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
				 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN  'N/A'
				 ELSE TRIM(CNTRY)
			END CNTRY  -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_LOC_A101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> --------------------';


		-- Loading silver.erp_PX_CAT_G1V2
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_PX_CAT_G1V2';
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data: silver.erp_PX_CAT_G1V2';
		INSERT INTO silver.erp_PX_CAT_G1V2(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
			)
			SELECT * FROM bronze.erp_PX_CAT_G1V2;
			SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> --------------------';

		SET @batch_end_time = GETDATE()
		PRINT '==================================================='
		PRINT 'Loading Silver Layer is Completed'
		PRINT '    - Total Load Duration: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'SECONDS'

	END TRY
	BEGIN CATCH
	PRINT '=========================================================';
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
	PRINT 'Error message'+ ERROR_MESSAGE();
		PRINT 'Error message'+ CAST (ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error message'+ CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================================';
	END CATCH
END
