/*
Script Purpose : 
This script will create a database name DataWarehous deleting the existing one, if exists.
Aditionally, it creates three schemas within the databases named: bronze, silver and gold.

Waenings:
If there is some database already exists with the database name DataWarehouse, it will  delete those and creates  a new one.
So, back up the database.
*/
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO
