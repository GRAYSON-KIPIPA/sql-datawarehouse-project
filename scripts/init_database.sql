--Check if Database Warehouse exists
IF EXISTS( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END
GO

USE MASTER;


--Create database Datawarehouse
CREATE DATABASE DataWarehouse;

USE Datawarehouse;

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

