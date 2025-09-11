 -- with in database bronze silver gold
WARNING ⚠️ 
 -- Running this script will drop the entire 'Datawarehouse' database if it exists.
  --All data in the database will be permanently deleted. Proceed with caution
   -- and ensure you have proper backups before running this script
-- create database "Data_Warehouse"
use master;
 create database DataWarehouse;

 use DataWarehouse;

create schema bronze;
go
create schema silver;
go
create schema gold;
go
