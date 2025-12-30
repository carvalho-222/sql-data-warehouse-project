/*
=======================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=======================================================

Script Purpose:
	This stored procedire loads data into the bronze layer from external csv files
	It performs the following tasks:
		- Truncates the tables before inserting any data;
		- Uses BULK INSERT in order to load the data into the bronze tables.

Parameters: 
	None.

Usage Example:
	EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY 
		
		SET @batch_start_time = GETDATE();

		PRINT('============================================');
		PRINT('LOADING BRONZE LAYER');
		PRINT('============================================');

		PRINT('--------------------------------------------');
		PRINT('LOADING CRM TABLES');
		PRINT('--------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncanting Table: bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT('>> Bulk Inserting Data Into Table: bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\emanu\OneDrive\Desktop\Emanuel Camargo de Carvalho\Projects\Data Engineering\data-warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- improves the performance, locks the whole table during the insertion
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's');
		PRINT('----------------');
		
		SET @start_time = GETDATE();
		PRINT('>> Truncanting Table: bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT('>> Bulk Inserting Data Into Table: bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\emanu\OneDrive\Desktop\Emanuel Camargo de Carvalho\Projects\Data Engineering\data-warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's');
		PRINT('----------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncanting Table: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT('>> Bulk Inserting Data Into Table: bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\emanu\OneDrive\Desktop\Emanuel Camargo de Carvalho\Projects\Data Engineering\data-warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's');
		PRINT('----------------');

		PRINT('--------------------------------------------');
		PRINT('LOADING ERP TABLES');
		PRINT('--------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncanting Table: bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT('>> Bulk Inserting Data Into Table: bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\emanu\OneDrive\Desktop\Emanuel Camargo de Carvalho\Projects\Data Engineering\data-warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's');
		PRINT('----------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncanting Table: bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT('>> Bulk Inserting Data Into Table: bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\emanu\OneDrive\Desktop\Emanuel Camargo de Carvalho\Projects\Data Engineering\data-warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's');
		PRINT('----------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncanting Table: bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT('>> Bulk Inserting Data Into Table: bronze.erp_px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\emanu\OneDrive\Desktop\Emanuel Camargo de Carvalho\Projects\Data Engineering\data-warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's');
		PRINT('----------------');

		SET @batch_end_time = GETDATE();
		PRINT('=====================================');
		PRINT('END LOADING BRONZE LAYER');
		PRINT('Total Load Time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 's');
		PRINT('=====================================');
	END TRY

	BEGIN CATCH
		PRINT('=============================================');
		PRINT('ERROR OCURRED DURING LOADING BRONZE LAYER');
		PRINT('Error Message' + ERROR_MESSAGE());
		PRINT('Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('Error State' + CAST(ERROR_STATE() AS NVARCHAR));
		PRINT('=============================================');
	END CATCH

END