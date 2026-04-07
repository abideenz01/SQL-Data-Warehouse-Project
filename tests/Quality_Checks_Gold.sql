/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

        --checking duplicates in primary key

SELECT 
cst_id,
COUNT(*)
FROM

(SELECT 
inf.cst_id,
inf.cst_key,
inf.cst_firstname,
inf.cst_lastname,
inf.cst_marital_status,
inf.cst_gndr,
inf.cst_create_date,
bir.BDATE,
bir.GEN,
loc.CNTRY
FROM Silver.crm_cust_info AS inf
LEFT JOIN Silver.erp_CUST_AZ12 AS bir
ON        inf.cst_key=bir.CID
LEFT JOIN Silver.erp_LOC_A101 AS loc
ON        inf.cst_key=loc.CID)t 
GROUP BY cst_id
HAVING COUNT(*)> 1
            -------Checking Distinct Values--------

SELECT DISTINCT
inf.cst_gndr,
bir.GEN,
CASE WHEN inf.cst_gndr!= 'n/a' THEN inf.cst_gndr  --CRM is the master for Gender Info
     ELSE COALESCE(bir.GEN,'n/a')
END AS New_Gender 
FROM Silver.crm_cust_info AS inf
LEFT JOIN Silver.erp_CUST_AZ12 AS bir
ON        inf.cst_key=bir.CID
LEFT JOIN Silver.erp_LOC_A101 AS loc
ON        inf.cst_key=loc.CID
ORDER BY 1,2

--------------------Checking Data Quality of Gold.dim_customers
SELECT DISTINCT 
Gender
FROM Gold.dim_customers

-------------------Foreign Key Integrity Check----------
SELECT *
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_customers cu
ON f.customer_key=cu.customer_key
LEFT JOIN Gold.dim_products pr
ON f.product_key=pr.product_key
WHERE cu.customer_key IS NULL OR pr.product_key IS NULL
