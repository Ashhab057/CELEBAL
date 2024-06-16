USE AdventureWorks2022;
GO

CREATE PROCEDURE InsertOrderDetails
    @OrderID INT,
    @ProductID INT,
    @UnitPrice DECIMAL(18, 2) = NULL,
    @Quantity INT,
    @Discount DECIMAL(18, 2) = 0
AS
BEGIN
    -- Declare variables
    DECLARE @ProductUnitPrice DECIMAL(18, 2);
    DECLARE @UnitsInStock INT;
    DECLARE @ReorderLevel INT;
    DECLARE @NewUnitsInStock INT;

    -- Begin transaction
    BEGIN TRANSACTION;

    -- Get the UnitPrice from the Product table if not provided
    IF @UnitPrice IS NULL
    BEGIN
        SELECT @ProductUnitPrice = ListPrice
        FROM Production.Product
        WHERE ProductID = @ProductID;
    END
    ELSE
    BEGIN
        SET @ProductUnitPrice = @UnitPrice;
    END

    -- Get current stock and reorder level
    SELECT @UnitsInStock = pi.Quantity, @ReorderLevel = ReorderPoint
    FROM Production.ProductInventory pi
    JOIN Production.Product p ON pi.ProductID = p.ProductID
    WHERE pi.ProductID = @ProductID;

    -- Check if there's enough stock
    IF @UnitsInStock < @Quantity
    BEGIN
        PRINT 'Not enough stock available. Transaction aborted.';
        ROLLBACK TRANSACTION;
        RETURN;
    END

    -- Insert order details
    INSERT INTO Sales.SalesOrderDetail (SalesOrderID, ProductID, UnitPrice, OrderQty, UnitPriceDiscount)
    VALUES (@OrderID, @ProductID, @ProductUnitPrice, @Quantity, @Discount);

    -- Check if the insert was successful
    IF @@ROWCOUNT = 0
    BEGIN
        PRINT 'Failed to place the order. Please try again.';
        ROLLBACK TRANSACTION;
        RETURN;
    END

    -- Adjust the quantity in stock
    SET @NewUnitsInStock = @UnitsInStock - @Quantity;
    UPDATE Production.ProductInventory
    SET Quantity = @NewUnitsInStock
    WHERE ProductID = @ProductID;

    -- Check if new stock level is below reorder level
    IF @NewUnitsInStock < @ReorderLevel
    BEGIN
        PRINT 'Warning: Stock level of ProductID ' + CAST(@ProductID AS NVARCHAR) + ' is below the reorder level.';
    END

    -- Commit the transaction
    COMMIT TRANSACTION;
END
GO




CREATE PROCEDURE UpdateOrderDetails
    @OrderID INT,
    @ProductID INT,
    @UnitPrice DECIMAL(18, 2) = NULL,
    @Quantity INT = NULL,
    @Discount DECIMAL(18, 2) = NULL
AS
BEGIN
    -- Declare variables to hold the current values
    DECLARE @CurrentUnitPrice DECIMAL(18, 2);
    DECLARE @CurrentQuantity INT;
    DECLARE @CurrentDiscount DECIMAL(18, 2);
    DECLARE @OldQuantity INT;
    DECLARE @NewUnitsInStock INT;
    DECLARE @UnitsInStock INT;
    DECLARE @ReorderLevel INT;

    -- Begin transaction
    BEGIN TRANSACTION;

    -- Retrieve current order details
    SELECT @CurrentUnitPrice = UnitPrice,
           @CurrentQuantity = OrderQty,
           @CurrentDiscount = UnitPriceDiscount
    FROM Sales.SalesOrderDetail
    WHERE SalesOrderID = @OrderID AND ProductID = @ProductID;

    -- Retrieve current stock and reorder level
    SELECT @UnitsInStock = pi.Quantity, @ReorderLevel = p.ReorderPoint
    FROM Production.ProductInventory pi
    JOIN Production.Product p ON pi.ProductID = p.ProductID
    WHERE pi.ProductID = @ProductID;

    -- Calculate the new quantity in stock
    SET @OldQuantity = @CurrentQuantity;
    SET @CurrentQuantity = ISNULL(@Quantity, @CurrentQuantity);
    SET @NewUnitsInStock = @UnitsInStock + @OldQuantity - @CurrentQuantity;

    -- Check if there's enough stock
    IF @NewUnitsInStock < 0
    BEGIN
        PRINT 'Not enough stock available. Transaction aborted.';
        ROLLBACK TRANSACTION;
        RETURN;
    END

    -- Update order details
    UPDATE Sales.SalesOrderDetail
    SET UnitPrice = ISNULL(@UnitPrice, @CurrentUnitPrice),
        OrderQty = @CurrentQuantity,
        UnitPriceDiscount = ISNULL(@Discount, @CurrentDiscount)
    WHERE SalesOrderID = @OrderID AND ProductID = @ProductID;

    -- Check if the update was successful
    IF @@ROWCOUNT = 0
    BEGIN
        PRINT 'Failed to update the order. Please try again.';
        ROLLBACK TRANSACTION;
        RETURN;
    END

    -- Adjust the quantity in stock
    UPDATE Production.ProductInventory
    SET Quantity = @NewUnitsInStock
    WHERE ProductID = @ProductID;

    -- Check if new stock level is below reorder level
    IF @NewUnitsInStock < @ReorderLevel
    BEGIN
        PRINT 'Warning: Stock level of ProductID ' + CAST(@ProductID AS NVARCHAR) + ' is below the reorder level.';
    END

    -- Commit the transaction
    COMMIT TRANSACTION;
END
GO



CREATE PROCEDURE GetOrderDetails
    @OrderID INT
AS
BEGIN
    -- Declare a variable to store the count of records
    DECLARE @RecordCount INT;

    -- Count the number of records for the given OrderID
    SELECT @RecordCount = COUNT(*)
    FROM Sales.SalesOrderDetail
    WHERE SalesOrderID = @OrderID;

    -- Check if any records are found
    IF @RecordCount = 0
    BEGIN
        PRINT 'The OrderID ' + CAST(@OrderID AS NVARCHAR) + ' does not exist.';
        RETURN 1;
    END
    ELSE
    BEGIN
        -- Select and return the order details
        SELECT SalesOrderID, ProductID, UnitPrice, OrderQty, UnitPriceDiscount
        FROM Sales.SalesOrderDetail
        WHERE SalesOrderID = @OrderID;
    END
END
GO





CREATE PROCEDURE DeleteOrderDetails
    @OrderID INT,
    @ProductID INT
AS
BEGIN
    -- Declare a variable to store the count of records
    DECLARE @RecordCount INT;

    -- Check if the given OrderID and ProductID exist in the Order Details table
    SELECT @RecordCount = COUNT(*)
    FROM Sales.SalesOrderDetail
    WHERE SalesOrderID = @OrderID AND ProductID = @ProductID;

    -- Validate the parameters
    IF @RecordCount = 0
    BEGIN
        PRINT 'Invalid parameters: The OrderID ' + CAST(@OrderID AS NVARCHAR) + ' and ProductID ' + CAST(@ProductID AS NVARCHAR) + ' combination does not exist.';
        RETURN -1;
    END

    -- Begin transaction
    BEGIN TRANSACTION;

    -- Delete the record from Order Details table
    DELETE FROM Sales.SalesOrderDetail
    WHERE SalesOrderID = @OrderID AND ProductID = @ProductID;

    -- Check if the delete was successful
    IF @@ROWCOUNT = 0
    BEGIN
        PRINT 'Failed to delete the order detail. Please try again.';
        ROLLBACK TRANSACTION;
        RETURN -1;
    END

    -- Commit the transaction
    COMMIT TRANSACTION;

    PRINT 'Order detail deleted successfully.';
END
GO





CREATE FUNCTION FormatDate(@InputDate DATETIME)
RETURNS VARCHAR(10)
AS
BEGIN
    RETURN (RIGHT('0' + CAST(MONTH(@InputDate) AS VARCHAR(2)), 2) + '/' +
            RIGHT('0' + CAST(DAY(@InputDate) AS VARCHAR(2)), 2) + '/' +
            CAST(YEAR(@InputDate) AS VARCHAR(4)))
END
GO



CREATE FUNCTION FormatDateYYYYMMDD(@InputDate DATETIME)
RETURNS VARCHAR(8)
AS
BEGIN
    RETURN (CAST(YEAR(@InputDate) AS VARCHAR(4)) +
            RIGHT('0' + CAST(MONTH(@InputDate) AS VARCHAR(2)), 2) +
            RIGHT('0' + CAST(DAY(@InputDate) AS VARCHAR(2)), 2))
END
GO

SELECT dbo.FormatDateYYYYMMDD('2006-11-21 23:34:05.920') AS FormattedDate;


GO



CREATE VIEW vwCustomerOrders AS
SELECT 
    CASE 
        WHEN c.PersonID IS NOT NULL THEN (p.FirstName + ' ' + p.LastName)
        WHEN c.StoreID IS NOT NULL THEN s.Name
        ELSE 'Unknown'
    END AS CompanyName,
    so.SalesOrderID AS OrderID,
    so.OrderDate,
    sod.ProductID,
    p.Name AS ProductName,
    sod.OrderQty AS Quantity,
    sod.UnitPrice,
    sod.OrderQty * sod.UnitPrice AS TotalPrice
FROM 
    Sales.SalesOrderHeader so
    JOIN Sales.SalesOrderDetail sod ON so.SalesOrderID = sod.SalesOrderID
    JOIN Production.Product p ON sod.ProductID = p.ProductID
    JOIN Sales.Customer c ON so.CustomerID = c.CustomerID
    LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN Sales.Store s ON c.StoreID = s.BusinessEntityID
GO






CREATE VIEW vwCustomerOrdersYesterday AS
SELECT 
    CASE 
        WHEN c.PersonID IS NOT NULL THEN ()
        WHEN c.StoreID IS NOT NULL THEN s.Name
        ELSE 'Unknown'
    END AS CompanyName,
    so.SalesOrderID AS OrderID,
    so.OrderDate,
    sod.ProductID,
    p.Name AS ProductName,
    sod.OrderQty AS Quantity,
    sod.UnitPrice,
    sod.OrderQty * sod.UnitPrice AS TotalPrice
FROM 
    Sales.SalesOrderHeader so
    JOIN Sales.SalesOrderDetail sod ON so.SalesOrderID = sod.SalesOrderID
    JOIN Production.Product p ON sod.ProductID = p.ProductID
    JOIN Sales.Customer c ON so.CustomerID = c.CustomerID
    LEFT JOIN Person.Person p ON c.PersonID = p.ProductSubcategoryID
    LEFT JOIN Sales.Store s ON c.StoreID = s.BusinessEntityID
WHERE 
    CAST(so.OrderDate AS DATE) = CAST(GETDATE() - 1 AS DATE) -- Orders placed yesterday
GO


CREATE VIEW MyProducts AS
SELECT 
    p.ProductID,
    p.ProductName,
    p.QuantityPerUnit,
    p.UnitPrice,
    s.CompanyName,
    c.CategoryName
FROM 
    Production.Products p
    JOIN Production.Suppliers s ON p.SupplierID = s.SupplierID
    JOIN Production.Categories c ON p.CategoryID = c.CategoryID
WHERE 
    p.Discontinued = 0; -- Exclude discontinued products
GO

CREATE DATABASE Northwind;

USE Northwind;
GO

CREATE TRIGGER DeleteOrderDetailsAndOrder
ON Orders
INSTEAD OF DELETE
AS
BEGIN
    -- Delete records from Order Details table for the deleted order
    DELETE FROM [Order Details]
    WHERE OrderID IN (SELECT OrderID FROM deleted);

    -- Delete the order from the Orders table
    DELETE FROM Orders
    WHERE OrderID IN (SELECT OrderID FROM deleted);
END
GO


USE Northwind;
GO

CREATE TRIGGER CheckStockAndFillOrder
ON [Order Details]
INSTEAD OF INSERT
AS
BEGIN
    -- Check if there is sufficient stock for each product in the order
    IF EXISTS (
        SELECT od.ProductID, od.Quantity, p.UnitsInStock
        FROM inserted od
        JOIN Products p ON od.ProductID = p.ProductID
        WHERE od.Quantity > p.UnitsInStock
    )
    BEGIN
        -- Insufficient stock, notify the user and refuse the order
        RAISERROR('Insufficient stock to fill the order. Order not placed.', 16, 1);
        ROLLBACK TRANSACTION;
    END
    ELSE
    BEGIN
        -- Sufficient stock, fill the order and decrement stock
        INSERT INTO [Order Details] (OrderID, ProductID, UnitPrice, Quantity, Discount)
        SELECT OrderID, ProductID, UnitPrice, Quantity, Discount
        FROM inserted;

        -- Decrement UnitsInStock in Products table for each product in the order
        UPDATE p
        SET p.UnitsInStock = p.UnitsInStock - od.Quantity
        FROM Products p
        JOIN inserted od ON p.ProductID = od.ProductID;
        
        COMMIT TRANSACTION;
    END
END
GO
