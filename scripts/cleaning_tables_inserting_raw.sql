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
