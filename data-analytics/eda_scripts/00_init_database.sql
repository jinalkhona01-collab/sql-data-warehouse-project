/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Database: DataWarehouseAnalytics

DROP DATABASE IF EXISTS "DataWarehouseAnalytics";
CREATE DATABASE "DataWarehouseAnalytics"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_India.1252'
    LC_CTYPE = 'English_India.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Create Schemas

CREATE SCHEMA gold;


CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number varchar(50),
	first_name varchar(50),
	last_name varchar(50),
	country varchar(50),
	marital_status varchar(50),
	gender varchar(50),
	birthdate date,
	create_date date
);


CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number varchar(50) ,
	product_name varchar(50) ,
	category_id varchar(50) ,
	category varchar(50) ,
	subcategory varchar(50) ,
	maintenance varchar(50) ,
	cost int,
	product_line varchar(50),
	start_date date 
);


CREATE TABLE gold.fact_sales(
	order_number varchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity int,
	price int 
);

TRUNCATE TABLE gold.dim_customers;
COPY gold.dim_customers FROM 'D:\Data Warehousing PRoject\sql-data-analytics-project\datasets\gold.dim_customers.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE gold.dim_products;
COPY gold.dim_products FROM 'D:\Data Warehousing PRoject\sql-data-analytics-project\datasets\gold.dim_products.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE gold.fact_sales;
COPY gold.fact_sales FROM 'D:\Data Warehousing PRoject\sql-data-analytics-project\datasets\gold.fact_sales.csv' DELIMITER ',' CSV HEADER;
