CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(order by cst_id) as customer_number, 
	cst.cst_id AS customer_id,
	cst.cst_key as customer_key,
	cst.cst_firstname as first_name,
	cst.cst_lastname as last_name,
	loc.cntry as country,
	sal.bdate as birth_date,
	cst.cst_marital_status as marital_status,
	CASE WHEN cst.cst_gndr!='n\a' THEN cst.cst_gndr
	ELSE COALESCE(SAL.gen,'n\a')
	END AS new_gen,	
	cst.cst_create_date
FROM silver.crm_cust_info cst
LEFT JOIN silver.erp_cust_az12 sal
ON sal.cid=cst.cst_key
LEFT JOIN silver.erp_loc_a101 loc
ON cst.cst_key=loc.cid

--for dimension table for products 
CREATE VIEW gold.dim_products AS
select
	ROW_NUMBER() over(order by prd_start_dt,prd_id) as product_number,
	prd.prd_id as product_id,
	prd.category_id as category_id,
	prd.prd_key as product_key,
	prd.prd_nm as product_name,
	px.cat as category,
	px.subcat as sub_category,
	px.maintenance as maintenance,
	prd.prd_cost as product_cost,
	prd.prd_line as prduct_type,
	prd.prd_start_dt as start_date
from silver.crm_prd_info prd
LEFT JOIN silver.erp_px_cat_g1v2 px
ON prd.category_id=px.id
where prd.prd_end_dt IS NULL

--fact table
CREATE VIEW gold.fact_sales AS
select 
	sls.sls_ord_num as order_number,
	pdd.product_number,
	cus.customer_number,
	sls.sls_order_dt as order_date,
	sls.sls_ship_dt as shiping_date,
	sls.sls_due_dt as due_date,
	sls.sls_sales sales_amount,
	sls.sls_quantity as sales_quantitiy,
	sls.sls_price as price
from silver.crm_sales_details sls
LEFT JOIN gold.dim_products pdd
ON sls.sls_prd_key=pdd.product_key
LEFT JOIN gold.dim_customers cus
on sls.sls_cust_id=cus.customer_id

