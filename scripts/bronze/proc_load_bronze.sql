CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time   DATETIME;

    BEGIN TRY
        PRINT '============================================';
        PRINT 'START LOADING THE BRONZE DATA';
        PRINT '============================================';

        PRINT '...........................................';
        PRINT 'Loading the CRM data sources.............';
        PRINT '...........................................';

        -----------------------------------------------------
        -- 1) Load bronze.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>>  TRUNCATING TABLE bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>>  Inserting into bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'A:\DW\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 
            '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '------------------------------------------------';

        -----------------------------------------------------
        -- 2) Load bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>>  TRUNCATING TABLE bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>>  Inserting into bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'A:\DW\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 
            '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '------------------------------------------------';

        -----------------------------------------------------
        -- 3) Load bronze.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>>  TRUNCATING TABLE bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>>  Inserting into bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'A:\DW\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 
            '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '------------------------------------------------';

        -----------------------------------------------------
        PRINT 'Loading the ERP Table Data............';

        -- 4) Load bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>>  TRUNCATING TABLE bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>>  Inserting into bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'A:\DW\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 
            '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '------------------------------------------------';

        -----------------------------------------------------
        -- 5) Load bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>>  TRUNCATING TABLE bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>>  Inserting into bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'A:\DW\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 
            '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '------------------------------------------------';

        -----------------------------------------------------
        -- 6) Load bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>>  TRUNCATING TABLE bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>>  INSERTING INTO bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'A:\DW\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 
            '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '------------------------------------------------';

    END TRY
    BEGIN CATCH
        PRINT '=============================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR(5));
        PRINT '=============================================';
    END CATCH
END;
GO

-- To run it:
EXEC bronze.load_bronze;
