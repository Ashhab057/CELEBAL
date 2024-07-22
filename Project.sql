USE [YourDatabase];

-- Step 1: Create Lineage Table
IF OBJECT_ID('Lineage', 'U') IS NOT NULL DROP TABLE Lineage;
CREATE TABLE Lineage (
    Lineage_Id BIGINT PRIMARY KEY IDENTITY(1,1),
    Source_System VARCHAR(100),
    Load_Stat_Datetime DATETIME,
    Load_EndDatetime DATETIME,
    Rows_at_Source INT,
    Rows_at_destination_Fact INT,
    Load_Status BIT
);

-- Step 2: Create Date Dimension Table
IF OBJECT_ID('Dim_Date', 'U') IS NOT NULL DROP TABLE Dim_Date;
CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Day_Number INT,
    Month_Name VARCHAR(50),
    Short_Month VARCHAR(3),
    Calendar_Month_Number INT,
    Calendar_Year INT,
    Fiscal_Month_Number INT,
    Fiscal_Year INT,
    Week_Number INT
);

DECLARE @StartDate DATE = '2000-01-01';
DECLARE @EndDate DATE = '2023-12-31';

WITH DateSeries AS (
    SELECT @StartDate AS DateValue
    UNION ALL
    SELECT DATEADD(DAY, 1, DateValue)
    FROM DateSeries
    WHERE DATEADD(DAY, 1, DateValue) <= @EndDate
)
INSERT INTO Dim_Date (DateKey, Date, Day_Number, Month_Name, Short_Month, Calendar_Month_Number, Calendar_Year, Fiscal_Month_Number, Fiscal_Year, Week_Number)
SELECT 
    YEAR(DateValue) * 10000 + MONTH(DateValue) * 100 + DAY(DateValue) AS DateKey,
    DateValue AS Date,
    DAY(DateValue) AS Day_Number,
    DATENAME(MONTH, DateValue) AS Month_Name,
    LEFT(DATENAME(MONTH, DateValue), 3) AS Short_Month,
    MONTH(DateValue) AS Calendar_Month_Number,
    YEAR(DateValue) AS Calendar_Year,
    CASE 
        WHEN MONTH(DateValue) >= 7 THEN MONTH(DateValue) - 6
        ELSE MONTH(DateValue) + 6 
    END AS Fiscal_Month_Number,
    CASE 
        WHEN MONTH(DateValue) >= 7 THEN YEAR(DateValue)
        ELSE YEAR(DateValue) - 1 
    END AS Fiscal_Year,
    DATEPART(WEEK, DateValue) AS Week_Number
FROM DateSeries
OPTION (MAXRECURSION 0);

-- Step 3: Create Dimension Tables
IF OBJECT_ID('Dim_Product', 'U') IS NOT NULL DROP TABLE Dim_Product;
CREATE TABLE Dim_Product (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    ProductName VARCHAR(255),
    Category VARCHAR(255),
    Lineage_Id BIGINT
);

IF OBJECT_ID('Dim_Customer', 'U') IS NOT NULL DROP TABLE Dim_Customer;
CREATE TABLE Dim_Customer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    CustomerName VARCHAR(255),
    CustomerAddress VARCHAR(255),
    CustomerEmail VARCHAR(255),
    CustomerPhone VARCHAR(50),
    EffectiveDate DATETIME,
    EndDate DATETIME,
    IsCurrent BIT,
    Lineage_Id BIGINT
);

IF OBJECT_ID('Dim_Employee', 'U') IS NOT NULL DROP TABLE Dim_Employee;
CREATE TABLE Dim_Employee (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    EmployeeName VARCHAR(255),
    EmployeeRole VARCHAR(255),
    EffectiveDate DATETIME,
    EndDate DATETIME,
    IsCurrent BIT,
    Lineage_Id BIGINT
);

IF OBJECT_ID('Dim_Geography', 'U') IS NOT NULL DROP TABLE Dim_Geography;
CREATE TABLE Dim_Geography (
    GeographyKey INT IDENTITY(1,1) PRIMARY KEY,
    GeographyID INT,
    Country VARCHAR(255),
    City VARCHAR(255),
    Population INT,
    PreviousPopulation INT,
    Lineage_Id BIGINT
);

-- Step 4: Create Fact Table
IF OBJECT_ID('Fact_Sales', 'U') IS NOT NULL DROP TABLE Fact_Sales;
CREATE TABLE Fact_Sales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    TransactionID INT,
    ProductKey INT,
    CustomerKey INT,
    EmployeeKey INT,
    GeographyKey INT,
    DateKey INT,
    Quantity INT,
    UnitPrice DECIMAL(10, 2),
    TotalAmount DECIMAL(10, 2),
    Lineage_Id BIGINT
);

-- Step 5: Create Insignia_staging_copy Table
IF OBJECT_ID('Insignia_staging_copy', 'U') IS NOT NULL DROP TABLE Insignia_staging_copy;
SELECT * INTO Insignia_staging_copy FROM Insignia_staging WHERE 1 = 0;

-- Step 6: Load Data into Dimensions from Insignia_staging_copy
-- Loading into Dim_Product
INSERT INTO Dim_Product (ProductID, ProductName, Category, Lineage_Id)
SELECT DISTINCT ProductID, ProductName, Category, 1
FROM Insignia_staging_copy;

-- Loading into Dim_Customer (SCD Type 2)
-- Insert new customers
INSERT INTO Dim_Customer (CustomerID, CustomerName, CustomerAddress, CustomerEmail, CustomerPhone, EffectiveDate, EndDate, IsCurrent, Lineage_Id)
SELECT CustomerID, CustomerName, CustomerAddress, CustomerEmail, CustomerPhone, GETDATE(), NULL, 1, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Customer AS dim
ON src.CustomerID = dim.CustomerID
WHERE dim.CustomerID IS NULL;

-- Update existing customers
UPDATE dim
SET dim.EndDate = GETDATE(), dim.IsCurrent = 0
FROM Dim_Customer AS dim
JOIN Insignia_staging_copy AS src
ON dim.CustomerID = src.CustomerID
WHERE dim.CustomerName <> src.CustomerName OR dim.CustomerAddress <> src.CustomerAddress OR dim.CustomerEmail <> src.CustomerEmail OR dim.CustomerPhone <> src.CustomerPhone AND dim.IsCurrent = 1;

-- Insert updated customers as new rows
INSERT INTO Dim_Customer (CustomerID, CustomerName, CustomerAddress, CustomerEmail, CustomerPhone, EffectiveDate, EndDate, IsCurrent, Lineage_Id)
SELECT CustomerID, CustomerName, CustomerAddress, CustomerEmail, CustomerPhone, GETDATE(), NULL, 1, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Customer AS dim
ON src.CustomerID = dim.CustomerID
WHERE dim.EndDate = GETDATE() AND dim.IsCurrent = 0;

-- Loading into Dim_Employee (SCD Type 2)
-- Insert new employees
INSERT INTO Dim_Employee (EmployeeID, EmployeeName, EmployeeRole, EffectiveDate, EndDate, IsCurrent, Lineage_Id)
SELECT EmployeeID, EmployeeName, EmployeeRole, GETDATE(), NULL, 1, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Employee AS dim
ON src.EmployeeID = dim.EmployeeID
WHERE dim.EmployeeID IS NULL;

-- Update existing employees
UPDATE dim
SET dim.EndDate = GETDATE(), dim.IsCurrent = 0
FROM Dim_Employee AS dim
JOIN Insignia_staging_copy AS src
ON dim.EmployeeID = src.EmployeeID
WHERE dim.EmployeeName <> src.EmployeeName OR dim.EmployeeRole <> src.EmployeeRole AND dim.IsCurrent = 1;

-- Insert updated employees as new rows
INSERT INTO Dim_Employee (EmployeeID, EmployeeName, EmployeeRole, EffectiveDate, EndDate, IsCurrent, Lineage_Id)
SELECT EmployeeID, EmployeeName, EmployeeRole, GETDATE(), NULL, 1, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Employee AS dim
ON src.EmployeeID = dim.EmployeeID
WHERE dim.EndDate = GETDATE() AND dim.IsCurrent = 0;

-- Loading into Dim_Geography (SCD Type 3)
-- Insert new geographies
INSERT INTO Dim_Geography (GeographyID, Country, City, Population, PreviousPopulation, Lineage_Id)
SELECT GeographyID, Country, City, Population, NULL, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Geography AS dim
ON src.GeographyID = dim.GeographyID
WHERE dim.GeographyID IS NULL;

-- Update existing geographies
UPDATE dim
SET dim.PreviousPopulation = dim.Population, dim.Population = src.Population
FROM Dim_Geography AS dim
JOIN Insignia_staging_copy AS src
ON dim.GeographyID = src.GeographyID
WHERE dim.Population <> src.Population;

-- Step 7: Load Data into Fact_Sales Table
INSERT INTO Fact_Sales (TransactionID, ProductKey, CustomerKey, EmployeeKey, GeographyKey, DateKey, Quantity, UnitPrice, TotalAmount, Lineage_Id)
SELECT 
    st.TransactionID,
    prod.ProductKey,
    cust.CustomerKey,
    emp.EmployeeKey,
    geo.GeographyKey,
    date.DateKey,
    st.Quantity,
    st.UnitPrice,
    st.TotalAmount,
    1
FROM Insignia_staging_copy st
JOIN Dim_Product prod ON st.ProductID = prod.ProductID
JOIN Dim_Customer cust ON st.CustomerID = cust.CustomerID
JOIN Dim_Employee emp ON st.EmployeeID = emp.EmployeeID
JOIN Dim_Geography geo ON st.GeographyID = geo.GeographyID
JOIN Dim_Date date ON st.SaleDate = date.Date;

-- Step 8: Handle Incremental Data Load
TRUNCATE TABLE Insignia_staging_copy;

-- Load Incremental Data
INSERT INTO Insignia_staging_copy (TransactionID, ProductID, CustomerID, EmployeeID, GeographyID, SaleDate, Quantity, UnitPrice, TotalAmount, Population)
SELECT * FROM Insignia_incremental;

-- Repeat ETL Steps for Incremental Data
-- Loading into Dimensions from Insignia_staging_copy
-- Loading into Dim_Product
INSERT INTO Dim_Product (ProductID, ProductName, Category, Lineage_Id)
SELECT DISTINCT ProductID, ProductName, Category, 1
FROM Insignia_staging_copy;

-- Loading into Dim_Customer (SCD Type 2)
-- Insert new customers
INSERT INTO Dim_Customer (CustomerID, CustomerName, CustomerAddress, CustomerEmail, CustomerPhone, EffectiveDate, EndDate, IsCurrent, Lineage_Id)
SELECT CustomerID, CustomerName, CustomerAddress, CustomerEmail, CustomerPhone, GETDATE(), NULL, 1, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Customer AS dim
ON src.CustomerID = dim.CustomerID
WHERE dim.CustomerID IS NULL;

-- Update existing customers
UPDATE dim
SET dim.EndDate = GETDATE(), dim.IsCurrent = 0
FROM Dim_Customer AS dim
JOIN Insignia_staging_copy AS src
ON dim.CustomerID = src.CustomerID
WHERE dim.CustomerName <> src.CustomerName OR dim.CustomerAddress <> src.CustomerAddress OR dim.CustomerEmail <> src.CustomerEmail OR dim.CustomerPhone <> src.CustomerPhone AND dim.IsCurrent = 1;

-- Insert updated customers as new rows
INSERT INTO Dim_Customer (CustomerID, CustomerName, CustomerAddress, CustomerEmail, CustomerPhone, EffectiveDate, EndDate, IsCurrent, Lineage_Id)
SELECT CustomerID, CustomerName, CustomerAddress, CustomerEmail, CustomerPhone, GETDATE(), NULL, 1, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Customer AS dim
ON src.CustomerID = dim.CustomerID
WHERE dim.EndDate = GETDATE() AND dim.IsCurrent = 0;

-- Loading into Dim_Employee (SCD Type 2)
-- Insert new employees
INSERT INTO Dim_Employee (EmployeeID, EmployeeName, EmployeeRole, EffectiveDate, EndDate, IsCurrent, Lineage_Id)
SELECT EmployeeID, EmployeeName, EmployeeRole, GETDATE(), NULL, 1, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Employee AS dim
ON src.EmployeeID = dim.EmployeeID
WHERE dim.EmployeeID IS NULL;

-- Update existing employees
UPDATE dim
SET dim.EndDate = GETDATE(), dim.IsCurrent = 0
FROM Dim_Employee AS dim
JOIN Insignia_staging_copy AS src
ON dim.EmployeeID = src.EmployeeID
WHERE dim.EmployeeName <> src.EmployeeName OR dim.EmployeeRole <> src.EmployeeRole AND dim.IsCurrent = 1;

-- Insert updated employees as new rows
INSERT INTO Dim_Employee (EmployeeID, EmployeeName, EmployeeRole, EffectiveDate, EndDate, IsCurrent, Lineage_Id)
SELECT EmployeeID, EmployeeName, EmployeeRole, GETDATE(), NULL, 1, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Employee AS dim
ON src.EmployeeID = dim.EmployeeID
WHERE dim.EndDate = GETDATE() AND dim.IsCurrent = 0;

-- Loading into Dim_Geography (SCD Type 3)
-- Insert new geographies
INSERT INTO Dim_Geography (GeographyID, Country, City, Population, PreviousPopulation, Lineage_Id)
SELECT GeographyID, Country, City, Population, NULL, 1
FROM Insignia_staging_copy AS src
LEFT JOIN Dim_Geography AS dim
ON src.GeographyID = dim.GeographyID
WHERE dim.GeographyID IS NULL;

-- Update existing geographies
UPDATE dim
SET dim.PreviousPopulation = dim.Population, dim.Population = src.Population
FROM Dim_Geography AS dim
JOIN Insignia_staging_copy AS src
ON dim.GeographyID = src.GeographyID
WHERE dim.Population <> src.Population;

-- Loading into Fact_Sales Table
INSERT INTO Fact_Sales (TransactionID, ProductKey, CustomerKey, EmployeeKey, GeographyKey, DateKey, Quantity, UnitPrice, TotalAmount, Lineage_Id)
SELECT 
    st.TransactionID,
    prod.ProductKey,
    cust.CustomerKey,
    emp.EmployeeKey,
    geo.GeographyKey,
    date.DateKey,
    st.Quantity,
    st.UnitPrice,
    st.TotalAmount,
    1
FROM Insignia_staging_copy st
JOIN Dim_Product prod ON st.ProductID = prod.ProductID
JOIN Dim_Customer cust ON st.CustomerID = cust.CustomerID
JOIN Dim_Employee emp ON st.EmployeeID = emp.EmployeeID
JOIN Dim_Geography geo ON st.GeographyID = geo.GeographyID
JOIN Dim_Date date ON st.SaleDate = date.Date;

-- Reconciliation Module
SELECT 
    'Insignia_staging' AS TableName,
    COUNT(*) AS RowCount
FROM Insignia_staging
UNION
SELECT 
    'Dim_Product' AS TableName,
    COUNT(*) AS RowCount
FROM Dim_Product
UNION
SELECT 
    'Dim_Customer' AS TableName,
    COUNT(*) AS RowCount
FROM Dim_Customer
UNION
SELECT 
    'Dim_Employee' AS TableName,
    COUNT(*) AS RowCount
FROM Dim_Employee
UNION
SELECT 
    'Dim_Geography' AS TableName,
    COUNT(*) AS RowCount
FROM Dim_Geography
UNION
SELECT 
    'Fact_Sales' AS TableName,
    COUNT(*) AS RowCount
FROM Fact_Sales;
