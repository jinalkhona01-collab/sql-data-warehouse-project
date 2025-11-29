/*
===============================================================================
Date Range Exploration 
===============================================================================
Purpose:
    - To determine the temporal boundaries of key data points.
    - To understand the range of historical data.

SQL Functions Used:
    - MIN(), MAX(), EXTRACT(), AGE()
===============================================================================
*/

--1. Explore all countries our customers come from and determine the sales data range.
-- Find the date of the first and last order
-- How many years of sales are available

SELECT 
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date, 
  EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) AS order_range_years, -- Calculate the difference in years between the first and last order dates (DATEDIFF)
  AGE(MAX(order_date), MIN(order_date)) AS order_range -- Calculate the full time difference between the first and last order dates
FROM gold.fact_sales;


-- 2. Find the youngest and the oldest customer and their ages.
SELECT
 MIN(birthdate) AS oldest_customer_birthdate,-- Oldest customer's birthdate
 EXTRACT(YEAR FROM AGE(NOW(), MIN(birthdate))) AS oldest_age, -- Calculate the age of the oldest customer
 MAX(birthdate) AS youngest_customer_birthdate,  --Youngest customer's birthdate
 EXTRACT(YEAR FROM AGE(NOW(), MAX(birthdate))) AS youngest_age -- Calculate the age of the Youngest customer
FROM gold.dim_customers;
