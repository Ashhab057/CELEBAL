-- Drop the database if it exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'CELEBAL')
BEGIN
    DROP DATABASE CELEBAL;
END
GO

-- Create the CELEBAL database
CREATE DATABASE CELEBAL;

GO

-- Use the CELEBAL database
USE CELEBAL;
USE MASTER
GO

-- Create the products table with order_date column and additional columns
CREATE TABLE products (
    orderid INT PRIMARY KEY,
    customer_company_name VARCHAR(255),
    company_location VARCHAR(255),
    product_name VARCHAR(255),
    amount_of_product INT,
    customer_contact_name VARCHAR(255),
    customer_firstname VARCHAR(255),
    customer_lastname VARCHAR(255),
    supervisor_firstname VARCHAR(255),
    customerid INT,
    faxnumber VARCHAR(50),
    employee_id INT,
	manager_id INT,
    order_date DATE, -- Added order_date column with DATE data type
    manager_name VARCHAR(255), -- Added manager name column
    employee_name VARCHAR(255), -- Added employee name column
    supervisor_name VARCHAR(255), -- Added supervisor name column
    customer_contact_number VARCHAR(50) -- Added customer contact number column
);
GO

-- Insert data into the products table with order_date, manager, employee, supervisor names, and customer contact numbers
INSERT INTO products (
    orderid, customer_company_name, company_location, product_name, amount_of_product, 
    customer_contact_name, customer_firstname, customer_lastname, supervisor_firstname, 
    customerid, faxnumber, employee_id,manager_id, order_date, manager_name, employee_name, supervisor_name, customer_contact_number
) VALUES 
(
    1, 'ABC Corp', 'New York', 'Widget A', 100, 
    'John Doe', 'John', 'Doe', 'Jane', 
    101, '123-456-7890', 1001,1002, '2024-05-01', 'Manager 1', 'Employee 1', 'Supervisor 1', '111-222-3333'
),
(
    2, 'XYZ Inc', 'UK', 'Gadget B', 50, 
    'Alice Smith', 'Alice', 'Smith', 'Bob', 
    102, '098-765-4321', 1002,1003 ,'2024-05-02', 'Manager 2', 'Employee 2', 'Supervisor 2', '222-333-4444'
),
-- Insert data for the remaining records
(
    3, 'Global Solutions', 'San Francisco', 'Gadget C', 75, 
    'Carlos Hernandez', 'Carlos', 'Hernandez', 'Maria', 
    103, '321-654-0987', 1003,1004,'2024-05-03', 'Manager 3', 'Employee 3', 'Supervisor 3', '333-444-5555'
),
(
    4, 'Tech Innovators', 'Austin', 'Widget B', 120, 
    'Sara Lee', 'Sara', 'Lee', 'David', 
    104, '456-789-0123', 1004,1005, '2024-05-04', 'Manager 4', 'Employee 4', 'Supervisor 4', '444-555-6666'
),
(
    5, 'Enterprise Holdings', 'Seattle', 'Widget D', 90, 
    'Michael Chen', 'Michael', 'Chen', 'Emily', 
    105, '654-321-0987', 1005,1006, '2024-05-05', 'Manager 5', 'Employee 5', 'Supervisor 5', '555-666-7777'
),
(
    6, 'Software Solutions', 'Boston', 'Gadget D', 60, 
    'Emily Davis', 'Emily', 'Davis', 'John', 
    106, '789-012-3456', 1006,1007, '2024-05-06', 'Manager 6', 'Employee 6', 'Supervisor 6', '666-777-8888'
),
(
    7, 'Healthcare Corp', 'USA', 'Widget E', 110, 
    'Anthony Brown', 'Anthony', 'Brown', 'Robert', 
    107, '890-123-4567', 1007,1008, '2024-05-07', 'Manager 7', 'Employee 7', 'Supervisor 7', '777-888-9999'
),
(
    8, 'Logistics LLC', 'Dallas', 'Gadget E', 40, 
    'Sophia Martinez', 'Sophia', 'Martinez', 'James', 
    108, '901-234-5678', 1008,1009, '2024-05-08', 'Manager 8', 'Employee 8', 'Supervisor 8', '888-999-0000'
),
(
    9, 'Manufacturing Inc', 'Denver', 'Widget F', 130, 
    'Liam Wilson', 'Liam', 'Wilson', 'William', 
    109, '012-345-6789', 1009,1010, '2024-05-09', 'Manager 9', 'Employee 9', 'Supervisor 9', '999-000-1111'
),
(
    10, 'Widgets Ltd', 'Chicago', 'Widget C', 200, 
    'Michael Johnson', 'Michael', 'Johnson', 'Sarah', 
    110, '555-555-5555', 1010,1011, '2024-05-10', 'Manager 10', 'Employee 10', 'Supervisor 10', '000-111-2222'
);
SELECT * from products;
-- Query to list all customers who live in Berlin or London
SELECT customerid, customer_company_name, company_location, customer_contact_name, customer_firstname, customer_lastname
FROM products
WHERE company_location IN ('Berlin', 'London');
GO
SELECT product_name FROM products ORDER BY product_name;
SELECT product_name
FROM products
WHERE product_name LIKE 'A%';


-- Query to list all customers who ever placed an order
SELECT DISTINCT customerid, customer_company_name, customer_contact_name, customer_firstname, customer_lastname
FROM products;


-- Query to list customers who live in London and bought chai
SELECT o.customerid, o.customer_company_name, o.company_location, o.customer_contact_name, o.customer_firstname, o.customer_lastname
FROM products o
JOIN (
    SELECT DISTINCT customerid
    FROM products
    WHERE company_location = 'London' AND product_name = 'Chai'
) c ON o.customerid = c.customerid;

-- Query to list all customers who never placed an order
SELECT DISTINCT c.customerid, c.customer_company_name
FROM products c
LEFT JOIN products o ON c.customerid = o.customerid
WHERE o.customerid IS NULL;

-- Query to list customers who bought 'tofi'
SELECT DISTINCT c.customerid, c.customer_company_name
FROM products c
JOIN products o ON c.customerid = o.customerid
JOIN products p ON o.product_name = p.product_name
WHERE p.product_name = 'tofi';

-- Query to retrieve details of the first order in the system
SELECT *
FROM products
ORDER BY orderid
OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

-- Query to find details of the most expensive order and its order date
SELECT o.orderid, o.customer_company_name, o.company_location, o.product_name, o.amount_of_product, o.customer_contact_name, o.customer_firstname, o.customer_lastname, o.supervisor_firstname, o.customerid, o.faxnumber, o.employee_id, o.order_date
FROM products o
JOIN (
    SELECT MAX(amount_of_product) AS max_amount
    FROM products
) AS max_order ON o.amount_of_product = max_order.max_amount;

-- Query to get OrderID and Average quantity of items for each order
SELECT orderid, AVG(amount_of_product) AS average_quantity
FROM products
GROUP BY orderid;

-- Query to get OrderID, minimum quantity, and maximum quantity for each order
SELECT orderid, MIN(amount_of_product) AS min_quantity, MAX(amount_of_product) AS max_quantity
FROM products
GROUP BY orderid;

-- Query to get a list of all managers and the total number of employees who report to them
SELECT m.manager_id, m.manager_name, COUNT(e.employee_id) AS total_employees
FROM products e
JOIN products m ON e.manager_id = m.employee_id
GROUP BY m.manager_id, m.manager_name;

-- Query to get OrderID and total quantity for each order with total quantity greater than 300
SELECT orderid, SUM(amount_of_product) AS total_quantity
FROM products
GROUP BY orderid
HAVING SUM(amount_of_product) > 300;

-- Query to get a list of all orders placed on or after December 31, 1996
SELECT *
FROM products
WHERE order_date >= '1996-12-31';

-- Query to get a list of all orders shipped to Canada
SELECT *
FROM products
WHERE company_location = 'Canada';
-- Query to get a list of all orders with order total > 200
SELECT orderid, SUM(amount_of_product) AS order_total
FROM products
GROUP BY orderid
HAVING SUM(amount_of_product) > 200;

-- Query to get a list of countries and total sales made in each country
SELECT  SUM(p.amount_of_product) AS total_sales
FROM products p
JOIN products c ON p.customerid = c.customerid;
-- Query to get a list of Customer ContactName and the number of orders they placed
SELECT c.customer_contact_name, COUNT(*) AS number_of_orders
FROM products p
JOIN products c ON p.customerid = c.customerid
GROUP BY c.customer_contact_name;

-- Query to get a list of Customer ContactNames who have placed more than 3 orders
SELECT c.customer_contact_name, COUNT(*) AS number_of_orders
FROM products p
JOIN products c ON p.customerid = c.customerid
GROUP BY c.customer_contact_name
HAVING COUNT(*) > 3;

-- Query to get a list of discontinued products ordered between 1/1/1997 and 1/1/1998
SELECT p.product_name, p.order_date
FROM products p
JOIN products dp ON p.product_id = dp.product_id
WHERE products = 1
AND p.order_date BETWEEN '1997-01-01' AND '1998-01-01';


-- Query to get a list of employee names and their supervisors' names
SELECT e.employee_name AS employee_firstname, e.employee_name AS employee_lastname,
       s.employee_name AS supervisor_firstname, s.employee_name AS supervisor_lastname
FROM products e
JOIN products s ON e.employee_name = s.employee_id;

-- Query to get a list of Employee IDs and their total sales
SELECT e.employee_id, e.employee_name, e.employee_name, SUM(p.amount_of_product) AS total_sales
FROM products e
JOIN products p ON e.employee_id = p.employee_id
GROUP BY e.employee_id, e.employee_name, e.employee_name;

-- Query to get a list of employees whose FirstName contains the character 'a'
SELECT *
FROM products
WHERE employee_name LIKE '%a%';
-- Query to get a list of managers with more than four people reporting to them
SELECT m.employee_name AS manager_id, m.employee_name AS manager_firstname, m.employee_name AS manager_lastname, 
       COUNT(e.employee_name) AS num_reports
FROM products m
JOIN products e ON m.employee_name = e.employee_name
GROUP BY m.employee_name, m.employee_name, m.employee_name
HAVING COUNT(e.employee_name) > 4;

-- Query to get a list of orders and their corresponding product names
SELECT o.orderid, o.product_name
FROM products o
JOIN products p ON o.orderid = p.orderid;



-- Query to find the best customer based on total amount of orders
SELECT TOP 1 customerid, customer_company_name, SUM(amount_of_product) AS total_orders
FROM products
GROUP BY customerid, customer_company_name
ORDER BY total_orders DESC;

-- Query to get the list of orders placed by the best customer
SELECT orderid, customer_company_name, product_name, amount_of_product
FROM products
WHERE customerid = (SELECT TOP 1 customerid
                     FROM products
                     GROUP BY customerid
                     ORDER BY SUM(amount_of_product) DESC);

					 -- Query to get the list of postal codes where the product "Tofu" was shipped
-- Query to get the list of product names that were shipped to France
SELECT DISTINCT p.product_name
FROM products o
JOIN products p ON o.orderid = p.orderid
WHERE o.company_location = 'France';

-- Query to get the list of product names and categories for the supplier 'Specialty Biscuits, Ltd.'
SELECT p.product_name, c.category_name
FROM suppliers s
JOIN products p ON s.supplierid = p.supplierid
JOIN categories c ON p.categoryid = c.categoryid
WHERE s.company_name = 'Specialty Biscuits, Ltd.';

-- Query to find the list of products that were never ordered
SELECT p.product_name
FROM products p
LEFT JOIN order_details od ON p.productid = od.productid
WHERE od.productid IS NULL;

-- List of products that were never ordered

SELECT p.product_name
FROM products p
LEFT JOIN order_details od ON p.productid = od.productid
WHERE od.productid IS NULL;

 --List of products where units in stock is less than 10 and units on order are 0.


SELECT product_name
FROM products
WHERE units_in_stock < 10 AND units_on_order = 0;

 --List of top 10 countries by sales

SELECT TOP 10 o.ship_country, SUM(od.quantity * od.unit_price) AS total_sales
FROM orders o
JOIN order_details od ON o.orderid = od.orderid
GROUP BY o.ship_country
ORDER BY total_sales DESC;

--Number of orders each employee has taken for customers with CustomerIDs between A and AO

SELECT e.employeeid, COUNT(o.orderid) AS number_of_orders
FROM employees e
JOIN orders o ON e.employeeid = o.employeeid
JOIN customers c ON o.customerid = c.customerid
WHERE c.customerid BETWEEN 'A' AND 'AO'
GROUP BY e.employeeid;

--Orderdate of most expensive order

SELECT o.orderid, o.order_date
FROM orders o
JOIN order_details od ON o.orderid = od.orderid
GROUP BY o.orderid, o.order_date
ORDER BY SUM(od.quantity * od.unit_price) DESC
OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;

--Product name and total revenue from that product

SELECT p.product_name, SUM(od.quantity * od.unit_price) AS total_revenue
FROM products p
JOIN order_details od ON p.productid = od.productid
GROUP BY p.product_name
ORDER BY total_revenue DESC;
--Supplierid and number of products offered

SELECT supplierid, COUNT(productid) AS number_of_products
FROM products
GROUP BY supplierid;

-- Top ten customers based on their business


SELECT TOP 10 c.customerid, c.contact_name, SUM(od.quantity * od.unit_price) AS total_business
FROM customers c
JOIN orders o ON c.customerid = o.customerid
JOIN order_details od ON o.orderid = od.orderid
GROUP BY c.customerid, c.contact_name
ORDER BY total_business DESC;

-- What is the total revenue of the company.


SELECT SUM(od.quantity * od.unit_price) AS total_revenue
FROM order_details od;
