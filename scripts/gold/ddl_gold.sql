/*
=================================================
DDL Script: Create Gold Views
=================================================
Script Purpose:
	This script creates views for the gold layer
	in the data warehouse. The gold layer represents
	the final dimension and fact tables (Star Schema).
	
	Each view performs transformations and combines data
	from the silver layer to produce a clean, enriched and
	business-ready dataset.

Usage:
	- These views can be queried directly for analytics
	and reporting.
===================================================
*/


-- ==============================================
-- Create Dimension Table: gold.dim_products
-- ==============================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.sls_prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.sls_prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cid AS category_id,
	COALESCE(pcg.cat, 'n/a') AS category,
	COALESCE(pcg.subcat, 'n/a') AS subcategory,
	COALESCE(pcg.maintenance, 'n/a') AS maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pcg
ON pn.cid = pcg.id
WHERE prd_end_dt IS NULL; -- Filter out all historical data - keep most recent
GO

-- ==============================================
-- Create Dimension Table: gold.dim_customers
-- ==============================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	COALESCE(la.cntry, 'n/a') AS country,
	CASE
		WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr -- CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ci.cst_marital_status AS marital_status,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid;
GO

-- ==============================================
-- Create Fact Table: gold.fact_sales
-- ==============================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	p.product_key,
	c.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS p
ON sd.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers AS c
ON sd.sls_cust_id = c.customer_id;

