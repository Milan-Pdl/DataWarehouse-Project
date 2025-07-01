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
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as customer_key,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
TRIM(prd_nm) as prd_nm,
isnull(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M'THEN 'Mountains'
	WHEN 'R' THEN 'Roads'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt as DATE) AS prd_start_dt,
LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt)-1 AS prd_end_dt
from bronze.crm_prd_info
