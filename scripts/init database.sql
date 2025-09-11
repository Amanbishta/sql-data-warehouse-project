 -- with in database bronze silver gold
WARNING ⚠️ 
 -- Running this script will drop the entire 'Datawarehouse' database if it exists.
  --All data in the database will be permanently deleted. Proceed with caution
   -- and ensure you have proper backups before running this script

use master;
-- Drop and recreate the 'Datawarehouse' database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
-- create database "Data_Warehouse"
 create database DataWarehouse;

 use DataWarehouse;

create schema bronze;
go
create schema silver;
go
create schema gold;
go
