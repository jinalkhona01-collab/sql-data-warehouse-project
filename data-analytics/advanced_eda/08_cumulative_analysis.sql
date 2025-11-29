/*
===============================================================================
Cumulative Analysis
===============================================================================
Purpose:
    - To calculate running totals or moving averages for key metrics.
    - To track performance over time cumulatively.
    - Useful for growth analysis or identifying long-term trends.

SQL Functions Used:
    - Window Functions: SUM() OVER(), AVG() OVER()
===============================================================================
*/

--8. Cumulative Analysis (Agg data progressively over time)
--Calculate the total sales per month and the running total of sales over time and moving average price
SELECT
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,  --window function
AVG(avg_price) OVER (ORDER BY order_date) AS moving_avg_price
FROM(
SELECT
DATE_TRUNC('month', order_date) AS order_date,  
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)
);

--Calculate the total sales per year and the running total of sales over time and moving average price
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,  --window function
AVG(avg_price) OVER (ORDER BY order_date) AS moving_avg_price
FROM(
SELECT
DATE_TRUNC('year', order_date) AS order_date,  
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('year', order_date)
);
