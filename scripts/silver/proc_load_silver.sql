-- ==========================
-- Transforming crm_cust_info
-- ==========================

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key, 
	cst_firstname, 
	cst_lastname, 
	cst_marital_status, 
	cst_gndr, 
	cst_create_date)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr,
	cst_create_date
FROM(
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
)t WHERE flag_last = 1;


-- ==========================
-- Transforming crm_prd_info
-- ==========================

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info(
	prd_id,
	sls_prd_key,
	cid,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
SELECT
	prd_id,
	SUBSTRING(TRIM(prd_key),7, LEN(prd_key)) AS sls_prd_key,
	REPLACE(LEFT(TRIM(prd_key), 5), '-', '_') AS cid,
	TRIM(prd_nm) AS prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,
	prd_start_dt,
	DATEADD(day, -1, LEAD(prd_start_dt, 1, NULL) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM(
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY prd_id ORDER BY prd_end_dt) AS flag_id
	FROM bronze.crm_prd_info
)t WHERE flag_id = 1

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_price,
	sls_quantity)
SELECT
	TRIM(sls_ord_num) AS sls_ord_num,
	TRIM(sls_prd_key) AS sls_prd_key,
	sls_cust_id,
	CASE
		WHEN LEN(sls_order_dt) <> 8 OR sls_order_dt <= 0 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE
		WHEN LEN(sls_ship_dt) <> 8 OR sls_ship_dt <= 0 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE
		WHEN LEN(sls_due_dt) <> 8 OR sls_due_dt <= 0 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE
		WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales <> sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END as sls_sales,
	CASE
		WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price,
	sls_quantity
FROM bronze.crm_sales_details

