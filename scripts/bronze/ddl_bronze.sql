
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;  
--Creating cust_info table
CREATE TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;  
 --Creating prd_info table
 CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50), 
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
 );


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;  
 --Creating Sales_Details table 
 --sls_ord_num	sls_prd_key	sls_cust_id	sls_order_dt	
 --sls_ship_dt	sls_due_dt	sls_sales	sls_quantity	sls_price
CREATE TABLE bronze.crm_sales_details(
    sls_ord_nun NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_date INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;  
--CID	BDATE	GEN
--Create table cust_az12
CREATE TABLE bronze.erp_cust_az12(
   cid NVARCHAR(50),
   dbate DATE,
   gen NVARCHAR(50),
);


IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101; 
CREATE TABLE bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

--ID	CAT	SUBCAT	MAINTENANCE
--Create table px_cat_g1v12
CREATE TABLE bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);


 
