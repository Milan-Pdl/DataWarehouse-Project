--first table

INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

select 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE 
	WHEN UPPER(TRIM(cst_marital_status))='S' then 'Single'
	WHEN UPPER(TRIM(cst_marital_status))='M' then 'Married'
	Else 'n\a' END AS cst_marital_status,--Normalizing the maritial_status into Understandable forms
CASE 
	WHEN UPPER(TRIM(cst_gndr))='M' then 'Male'
	WHEN UPPER(TRIM(cst_gndr))='F' then 'Female'
	Else 'n\a' END AS cst_gndr,--Normalizing the gender into Understandable forms
cst_create_date
from(
SELECT *,
	DENSE_RANK() over(partition by cst_id order by cst_create_date desc) as rnk 
	FROM bronze.crm_cust_info
where cst_id is not null)a where rnk=1--getting the current value of customer id(recent records of the customer)

--second table product information
INSERT INTO silver.crm_prd_info(
	   [prd_id]
      ,[category_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt])
select 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as customer_key,--
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
TRIM(prd_nm) as prd_nm,
isnull(prd_cost,0) as prd_cost,-- Handeling the missing information
CASE UPPER(TRIM(prd_line))
	WHEN 'M'THEN 'Mountains'
	WHEN 'R' THEN 'Roads'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,-- Refedining the abbrevration
CAST(prd_start_dt as DATE) AS prd_start_dt,--we are data casting for proper date
LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt)-1 AS prd_end_dt-- doing a dataenrichment
from bronze.crm_prd_info

--third table messed up for god sake
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

select
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE
	WHEN sls_order_dt=0 or LEN(sls_order_dt)!=8 then NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE
	WHEN sls_ship_dt=0 or LEN(sls_ship_dt)!=8 then NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE
	WHEN sls_due_dt=0 or LEN(sls_due_dt)!=8 then NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_sales,
CASE
	WHEN sls_sales<0 OR sls_sales IS NULL or sls_sales!=sls_quantity*sls_price THEN (sls_quantity*ABS(sls_price))
	ELSE sls_sales
END AS sls_sales,	
sls_quantity,
CASE	
	WHEN sls_price is NULL OR sls_price<=0 THEN NULLIF(sls_sales/sls_quantity,0)
	ELSE sls_price
END AS sls_price
from bronze.crm_sales_details
 
