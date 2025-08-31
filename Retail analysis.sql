/* =========================================================
   Retail Analytics Project - FULL BUILD + LOAD + ANALYTICS
   ========================================================= */

-- Create and switch DB
IF DB_ID('Retail_Analytics_Project') IS NULL
BEGIN
    CREATE DATABASE Retail_Analytics_Project;
END
GO
USE Retail_Analytics_Project;
GO

/* =========================
   1) BASE TABLES (IF MISSING)
   ========================= */

-- Customers
IF OBJECT_ID('dbo.customers','U') IS NULL
BEGIN
    CREATE TABLE dbo.customers (
      customer_id INT PRIMARY KEY,
      customer_name VARCHAR(100),
      gender CHAR(1),
      age INT,
      city VARCHAR(100),
      state VARCHAR(100),
      region VARCHAR(50),
      signup_date DATE,
      segment VARCHAR(20)
    );
END

-- Products
IF OBJECT_ID('dbo.products','U') IS NULL
BEGIN
    CREATE TABLE dbo.products (
      product_id INT PRIMARY KEY,
      category VARCHAR(100),
      subcategory VARCHAR(100),
      product_name VARCHAR(150),
      cost DECIMAL(10,2),
      price DECIMAL(10,2)
    );
END

-- Stores
IF OBJECT_ID('dbo.stores','U') IS NULL
BEGIN
    CREATE TABLE dbo.stores (
      store_id INT PRIMARY KEY,
      store_name VARCHAR(100),
      city VARCHAR(100),
      state VARCHAR(100),
      region VARCHAR(50)
    );
END

-- Transactions (child of customers/products/stores)
IF OBJECT_ID('dbo.transactions','U') IS NULL
BEGIN
    CREATE TABLE dbo.transactions (
      transaction_id INT PRIMARY KEY,
      transaction_date DATE NOT NULL,
      customer_id INT NOT NULL,
      store_id INT NOT NULL,
      product_id INT NOT NULL,
      quantity INT,
      discount_pct DECIMAL(5,2),
      payment_method VARCHAR(50),
      gross_amount DECIMAL(12,2)
    );

    ALTER TABLE dbo.transactions
      ADD CONSTRAINT FK_transactions_customers
          FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id),
          CONSTRAINT FK_transactions_stores
          FOREIGN KEY (store_id) REFERENCES dbo.stores(store_id),
          CONSTRAINT FK_transactions_products
          FOREIGN KEY (product_id) REFERENCES dbo.products(product_id);
END

-- Inventory (child of products)
IF OBJECT_ID('dbo.inventory','U') IS NULL
BEGIN
    CREATE TABLE dbo.inventory (
      product_id INT NOT NULL,
      [date] DATE NOT NULL,
      opening_stock INT,
      purchased INT,
      sold INT,
      closing_stock INT,
      CONSTRAINT PK_inventory PRIMARY KEY (product_id, [date]),
      CONSTRAINT FK_inventory_products FOREIGN KEY (product_id) REFERENCES dbo.products(product_id)
    );
END
GO

/* =========================================
   2) MASTER DATA LOADS (STAGING + MERGE)
   ========================================= */

-- ========== CUSTOMERS ==========
IF OBJECT_ID('dbo.customers_staging','U') IS NOT NULL DROP TABLE dbo.customers_staging;
CREATE TABLE dbo.customers_staging (
  customer_id INT,
  customer_name VARCHAR(100),
  gender CHAR(1),
  age INT,
  city VARCHAR(100),
  state VARCHAR(100),
  region VARCHAR(50),
  signup_date DATE,
  segment VARCHAR(20)
);

BULK INSERT dbo.customers_staging
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\customers.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

MERGE dbo.customers AS target
USING dbo.customers_staging AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET customer_name = source.customer_name,
               gender = source.gender,
               age = source.age,
               city = source.city,
               state = source.state,
               region = source.region,
               signup_date = source.signup_date,
               segment = source.segment
WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, customer_name, gender, age, city, state, region, signup_date, segment)
    VALUES (source.customer_id, source.customer_name, source.gender, source.age,
            source.city, source.state, source.region, source.signup_date, source.segment);

DROP TABLE dbo.customers_staging;
GO

-- ========== PRODUCTS ==========
IF OBJECT_ID('dbo.products_staging','U') IS NOT NULL DROP TABLE dbo.products_staging;
CREATE TABLE dbo.products_staging (
  product_id INT,
  category VARCHAR(100),
  subcategory VARCHAR(100),
  product_name VARCHAR(150),
  cost DECIMAL(10,2),
  price DECIMAL(10,2)
);

BULK INSERT dbo.products_staging
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\products.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

MERGE dbo.products AS target
USING dbo.products_staging AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET category = source.category,
               subcategory = source.subcategory,
               product_name = source.product_name,
               cost = source.cost,
               price = source.price
WHEN NOT MATCHED BY TARGET THEN
    INSERT (product_id, category, subcategory, product_name, cost, price)
    VALUES (source.product_id, source.category, source.subcategory,
            source.product_name, source.cost, source.price);

DROP TABLE dbo.products_staging;
GO

-- ========== STORES ==========
IF OBJECT_ID('dbo.stores_staging','U') IS NOT NULL DROP TABLE dbo.stores_staging;
CREATE TABLE dbo.stores_staging (
  store_id INT,
  store_name VARCHAR(100),
  city VARCHAR(100),
  state VARCHAR(100),
  region VARCHAR(50)
);

BULK INSERT dbo.stores_staging
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\stores.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

MERGE dbo.stores AS target
USING dbo.stores_staging AS source
ON target.store_id = source.store_id
WHEN MATCHED THEN
    UPDATE SET store_name = source.store_name,
               city = source.city,
               state = source.state,
               region = source.region
WHEN NOT MATCHED BY TARGET THEN
    INSERT (store_id, store_name, city, state, region)
    VALUES (source.store_id, source.store_name, source.city, source.state, source.region);

DROP TABLE dbo.stores_staging;
GO

/* ==================================
   3) FACT/DAILY TABLES RELOAD (SAFE)
   ================================== */

-- Transactions: child table ? TRUNCATE is allowed (no children reference it)
TRUNCATE TABLE dbo.transactions;

BULK INSERT dbo.transactions
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\transactions.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);

-- Inventory: child of products ? TRUNCATE is allowed (no children reference it)
TRUNCATE TABLE dbo.inventory;

BULK INSERT dbo.inventory
FROM 'C:\Users\rithi\OneDrive\Documents\fielsss\Airbin\Sales analysis\inventory.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO


