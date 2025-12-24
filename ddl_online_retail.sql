/*
=============================================================
Create Database and Schema
=============================================================
Script Purpose:
    This script creates a new database named 'online_retail' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Then creates three schemas a
    within the database: 'bronze', 'silver', and 'gold'.
	.
	every schema is intended to represent different layers of data processing and storage.
	a bronze layer for raw data, a silver layer for cleaned and processed data, and a gold layer for aggregated and business-ready data.


Important WARNING:
    Running this script will drop the entire 'online_retail' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script
*/

USE master;
GO
---------------------------------------------------------
-- Drop and recreate the 'Online_retail' database
---------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Online_retail')
BEGIN
    ALTER DATABASE Online_retail SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Online_retail;
END;
GO
--------------------------------------------------------
-- Create the 'Online_retail' database
--------------------------------------------------------
CREATE DATABASE Online_retail;
GO

USE Online_retail;
GO

-- Create Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO 
 
CREATE SCHEMA gold;
GO
