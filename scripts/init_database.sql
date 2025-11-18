/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'. 
    Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
/*

-- Create the 'DataWarehouse' database

CREATE DATABASE "DataWarehouse"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;


-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
