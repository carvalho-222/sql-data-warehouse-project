/*
===========================
Create Database and Schemas
===========================

Script Purpose:
	This script creates a new DB named DataWarehouse after checking if already exists.
	If so, it is dropped and reacreated. Additionally, it sets up three schemas within the
	DB, the layers: bronze, silver and gold.

Warning:
	Running this script will drop the DataWarehouse DB if it exists.
	All the data will be lost. Proceed with caution and ensure you have
	propor backups before running this script.
*/

USE master;
GO

-- Drop and recreate the DataWarehouse DB
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Creates the DataWarehouse DB
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Creating Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO