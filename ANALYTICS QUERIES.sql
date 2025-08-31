/* ==========================
   4) ANALYTICS QUERIES
   ========================== */
USE  Retail_Analytics_Project;
-- 1) Total Revenue by Month (works on all SQL Server versions)
SELECT 
    CAST(DATEFROMPARTS(YEAR(transaction_date), MONTH(transaction_date), 1) AS DATE) AS [month],
    SUM(gross_amount) AS revenue
FROM dbo.transactions
GROUP BY DATEFROMPARTS(YEAR(transaction_date), MONTH(transaction_date), 1)
ORDER BY [month];

-- 2) Top 10 Products by Revenue
SELECT TOP 10
    p.product_id,
    p.product_name,
    p.category,
    SUM(t.gross_amount) AS revenue
FROM dbo.transactions t
JOIN dbo.products p 
    ON p.product_id = t.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC;

-- 3) Customer Segmentation: High / Medium / Low based on total spend
WITH spend AS (
    SELECT 
        c.customer_id, 
        c.customer_name, 
        SUM(t.gross_amount) AS total_spend
    FROM dbo.customers c
    JOIN dbo.transactions t 
        ON t.customer_id = c.customer_id
    GROUP BY c.customer_id, c.customer_name
),
bounds AS (
    SELECT 
        PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY total_spend) OVER () AS p33,
        PERCENTILE_CONT(0.66) WITHIN GROUP (ORDER BY total_spend) OVER () AS p66
    FROM spend
)
SELECT 
    s.customer_id, 
    s.customer_name, 
    s.total_spend,
    CASE
        WHEN s.total_spend >= b.p66 THEN 'High'
        WHEN s.total_spend >= b.p33 THEN 'Medium'
        ELSE 'Low'
    END AS segment
FROM spend s
CROSS JOIN (SELECT DISTINCT p33, p66 FROM bounds) b
ORDER BY s.total_spend DESC;

-- 4) Inventory Stockouts (closing stock below 10)
SELECT 
    i.[date], 
    i.product_id, 
    p.product_name, 
    i.closing_stock
FROM dbo.inventory i
JOIN dbo.products p 
    ON p.product_id = i.product_id
WHERE i.closing_stock < 10
ORDER BY i.[date] DESC, i.closing_stock ASC;

-- 5) Store Performance KPIs
SELECT 
    s.store_id, 
    s.store_name, 
    s.city, 
    s.region,
    COUNT(DISTINCT t.transaction_id) AS orders,
    SUM(t.gross_amount) AS revenue,
    AVG(t.discount_pct) AS avg_discount
FROM dbo.transactions t
JOIN dbo.stores s 
    ON s.store_id = t.store_id
GROUP BY s.store_id, s.store_name, s.city, s.region
ORDER BY revenue DESC;
