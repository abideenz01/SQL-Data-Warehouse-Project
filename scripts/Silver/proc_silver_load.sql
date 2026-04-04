/*
-----------------------------------------------------------------------
Stored Procedure: Load Silver Layer (Bronze Layer-->Silver Layer)
-----------------------------------------------------------------------
Script Purpose:  This stored procedure performs the ETL (Extract, Transform, Load) process to populate the “Silver” schema tables the “Bronze” schema.

Action Performed: 

1.Truncates Silver tables.
2.Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters: 
- None
This stored procedure does not accept any parameters or return any values.

Execution Command: 
EXEC Silver.load_Silver;

*/

          ---Data Cleansing and Transformations--
--Executing Stored Procedure of Silver Layer
EXEC Silver.load_Silver;

CREATE OR ALTER PROCEDURE Silver.load_Silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    BEGIN TRY 
    SET @start_time=GETDATE()
    PRINT'======================================';
    PRINT'Loading Silver Layer';
    PRINT'=====================================';
    PRINT'>> Truncating table Silver.crm_cust_info';
    TRUNCATE TABLE Silver.crm_cust_info
    PRINT'>>Inserting data into Silver.crm_cust_info';
    INSERT INTO Silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)

     --1. 1st table Data Cleansing and Transformations
    SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname, --TRIM removes any leading and trailing spaces 
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status))= 'M' THEN 'Married'
         ELSE 'n/a'
    END AS cst_marital_status , --Normalize martital status values to readbale format
    CASE WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'
         ELSE 'n/a'
    END AS cst_gndr, --Normalzie gender values to readable format
    cst_create_date

    FROM 

    (SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
    FROM Bronze.crm_cust_info
    WHERE cst_id IS NOT NULL   --returns unique primary key
    )t WHERE  flag_last= 1; --removes duplicates and returns latest data 
    SET @end_time= GETDATE();
        PRINT'Duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        PRINT'---------'
    PRINT'--------------------------------------------';
    SET @start_time= GETDATE()
    PRINT'>> Truncating table Silver.crm_prd_info';
    TRUNCATE TABLE Silver.crm_prd_info
    PRINT'>>Inserting data into Silver.crm_prd_info';
    INSERT INTO Silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
    --2. 2nd table Data Cleansing and Transformations
    SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, --Replace '-' with '_' to match with ID of Bronze.erp.PX_CAT_G1V2 table
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, --need prd_key to match with sls_prd_key from Bronze.crm_sales_details
    prd_nm,
    ISNULL(prd_cost,0) prd_cost,
    CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Touring'
         ELSE 'n/a'
    END prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE)  prd_end_dt --End Date= Start Date of next record - 1
    FROM Bronze.crm_prd_info;
    SET @end_time= GETDATE()
    PRINT'Duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

    PRINT'----------------------------------------';
    SET @start_time= GETDATE()
    PRINT'>>Truncating table Silver.crm_sales_details';
    TRUNCATE TABLE Silver.crm_sales_details
    PRINT'>>Inserting data into Silver.crm_sales_details';
    INSERT INTO Silver.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
    --3. 3rd table Data Cleansing and Transformations

    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL 
         ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) --casting integer to varchar and then varchar to date because direct conversion from integer to date is not possible in sql server
    END AS sls_order_dt,
    CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) --casting integer to varchar and then varchar to date because direct conversion from integer to date is not possible in sql server
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE) --casting integer to varchar and then varchar to date because direct conversion from integer to date is not possible in sql server
    END AS sls_due_dt,
    CASE WHEN sls_sales<=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)  --ABS provides absolute number
         ELSE sls_sales  --Recalculating sales if original value is missing or incorrect
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price<=0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quantity,0) --derive price if original value is invalid
         ELSE sls_price
    END AS sls_price
    FROM Bronze.crm_sales_details;
    SET @end_time= GETDATE()
    PRINT'Duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

    PRINT'----------------------------------------';
    SET @start_time= GETDATE()
    PRINT'>>Truncating table Silver.erp_CUST_AZ12';
    TRUNCATE TABLE Silver.erp_CUST_AZ12
    PRINT'>>Inserting data into Silver.erp_CUST_AZ12';
    INSERT INTO Silver.erp_CUST_AZ12(CID,BDATE,GEN)

    --4. Fourth table Data Cleansing and Transformations
    SELECT 
     CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
          ELSE CID
     END AS CID,
     CASE WHEN BDATE> GETDATE() THEN NULL
          ELSE BDATE
     END BDATE,
     CASE WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
          WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
          ELSE 'n/a'
    END GEN
    FROM Bronze.erp_CUST_AZ12;
    SET @end_time= GETDATE()
    PRINT'Duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

    PRINT'----------------------------------------';
    SET @start_time= GETDATE()
    PRINT'>>Truncating table Silver.erp_LOC_A101';
    TRUNCATE TABLE Silver.erp_LOC_A101
    PRINT'>>Inserting data into Silver.erp_LOC_A101';
    INSERT INTO Silver.erp_LOC_A101(CID,CNTRY)
     --5.5th table Data Cleansing and Transformations
    SELECT 
    REPLACE(CID,'-','') AS CID,
    CASE  WHEN UPPER(TRIM(CNTRY))='DE' THEN 'Germany'
          WHEN UPPER(TRIM(CNTRY)) IN ('US','USA') THEN 'United States'
          WHEN CNTRY= ' ' OR CNTRY IS NULL THEN 'n/a'
          ELSE TRIM(CNTRY)
    END AS CNTRY
    FROM Bronze.erp_LOC_A101;
    SET @end_time= GETDATE()
    PRINT'Duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

    PRINT'----------------------------------------';
    SET @start_time= GETDATE()
    PRINT'>>Truncating table Silver.erp_PX_CAT_G1V2';
    TRUNCATE TABLE Silver.erp_PX_CAT_G1V2
    PRINT'>>Inserting data into Silver.erp_PX_CAT_G1V2';
    INSERT INTO Silver.erp_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
     --6. 6th table Data Cleansing and Transformations
     SELECT 
     CASE WHEN ID='CO_PD' THEN 'n/a'
          ELSE ID
     END AS ID,
     CAT,
     SUBCAT,
     MAINTENANCE
     FROM Bronze.erp_PX_CAT_G1V2;
     SET @end_time= GETDATE()
     PRINT'Duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
     PRINT'---------------------------'
     PRINT'Loading of Silver Layer is completed'
     SET @end_time= GETDATE()
     PRINT'Duration of loading silver layer is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
     END TRY
         BEGIN CATCH 
                PRINT'======================================';
                PRINT'Error Message:' + ERROR_MESSAGE();
                PRINT'Error Message:' + CAST(ERROR_NUMBER()AS NVARCHAR);
                PRINT'Error Message:' + CAST(ERROR_STATE()AS NVARCHAR);
                PRINT'======================================';
          END CATCH 
END 










