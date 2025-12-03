USE [Cohort Subs];
GO


-- 1. CLEAN UP
-- Reset temp tables to allow re-running the script
IF OBJECT_ID('tempdb..#online_retail_main') IS NOT NULL DROP TABLE #online_retail_main;
IF OBJECT_ID('tempdb..#cohort') IS NOT NULL DROP TABLE #cohort;
IF OBJECT_ID('tempdb..#cohort_retention') IS NOT NULL DROP TABLE #cohort_retention;
IF OBJECT_ID('tempdb..#cohort_pivot') IS NOT NULL DROP TABLE #cohort_pivot;


-- 2. DATA PREP & CLEANING
-- Using CTEs to filter and de-duplicate raw data before analysis
;WITH online_retail AS
(
    -- Exclude records without CustomerID (can't track retention for guests)
	SELECT [InvoiceNo], [StockCode], [Description], [Quantity], [InvoiceDate], [UnitPrice], [CustomerID], [Country]
	FROM [dbo].[Online Retail New]
	WHERE CustomerID != 0
)
, quantity_unit_price AS 
(
	-- Remove returns (negative Qty) and bad data (0 UnitPrice)
    -- We only want valid sales transactions
	SELECT *
	FROM online_retail
	WHERE Quantity > 0 AND UnitPrice > 0
)
, dup_check AS
(
	-- Flag duplicates based on Invoice, StockCode, and Quantity
	SELECT * , ROW_NUMBER() OVER (PARTITION BY InvoiceNo, StockCode, Quantity ORDER BY InvoiceDate) AS dup_flag
	FROM quantity_unit_price
)
-- Store clean unique data into temp table
SELECT *
INTO #online_retail_main
FROM dup_check
WHERE dup_flag = 1;


-- 3. DEFINE COHORT MONTH
-- Find the very first purchase month for each customer
-- Normalize date to the 1st of the month (e.g., 2023-01-15 -> 2023-01-01)
SELECT
	CustomerID,
	MIN(InvoiceDate) AS first_purchase_date,
	DATEFROMPARTS(YEAR(MIN(InvoiceDate)), MONTH(MIN(InvoiceDate)), 1) AS Cohort_Date
INTO #cohort
FROM #online_retail_main
GROUP BY CustomerID;


-- 4. CALCULATE RETENTION INDEX
-- Calculate month gap: Month 1 (Join Month), Month 2, etc.
SELECT
	mmm.*,
	cohort_index = year_diff * 12 + month_diff + 1 
INTO #cohort_retention
FROM
	(
		SELECT
			mm.*,
			year_diff = invoice_year - cohort_year,
			month_diff = invoice_month - cohort_month
		FROM
			(
				SELECT
					m.*,
					c.Cohort_Date,
					YEAR(m.InvoiceDate) AS invoice_year,
					MONTH(m.InvoiceDate) AS invoice_month,
					YEAR(c.Cohort_Date) AS cohort_year,
					MONTH(c.Cohort_Date) AS cohort_month
				FROM #online_retail_main m
				LEFT JOIN #cohort c ON m.CustomerID = c.CustomerID
			)mm
	)mmm;


-- 5. PIVOT: ABSOLUTE NUMBERS
-- Create the standard cohort triangular matrix
SELECT *
INTO #cohort_pivot
FROM(
	SELECT DISTINCT 
		CustomerID,
		Cohort_Date,
		cohort_index
	FROM #cohort_retention
)tbl
PIVOT(
	COUNT(CustomerID)
	FOR Cohort_Index IN 
		([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13])
)AS pivot_table;

-- Check absolute values
SELECT * FROM #cohort_pivot ORDER BY Cohort_Date;


-- 6. PIVOT: RETENTION RATES (%)
-- Convert to percentages.
-- Note: Added NULLIF to [1] to handle potential divide-by-zero errors safely.
SELECT Cohort_Date ,
	(1.0 * [1]/NULLIF([1],0) * 100) AS [1], 
    1.0 * [2]/NULLIF([1],0) * 100 AS [2], 
    1.0 * [3]/NULLIF([1],0) * 100 AS [3],  
    1.0 * [4]/NULLIF([1],0) * 100 AS [4],  
    1.0 * [5]/NULLIF([1],0) * 100 AS [5], 
    1.0 * [6]/NULLIF([1],0) * 100 AS [6], 
    1.0 * [7]/NULLIF([1],0) * 100 AS [7], 
	1.0 * [8]/NULLIF([1],0) * 100 AS [8], 
    1.0 * [9]/NULLIF([1],0) * 100 AS [9], 
    1.0 * [10]/NULLIF([1],0) * 100 AS [10],    
    1.0 * [11]/NULLIF([1],0) * 100 AS [11],  
    1.0 * [12]/NULLIF([1],0) * 100 AS [12],  
	1.0 * [13]/NULLIF([1],0) * 100 AS [13]
FROM #cohort_pivot
ORDER BY Cohort_Date;


-- 7. DYNAMIC PIVOT (OPTIONAL)
-- Useful if the dataset grows and we don't want to hardcode 
DECLARE 
    @columns NVARCHAR(MAX) = '',
	@sql     NVARCHAR(MAX) = '';

-- Build column list dynamically
SELECT 
    @columns += QUOTENAME(cohort_index) + ','
FROM 
    (SELECT DISTINCT cohort_index FROM #cohort_retention) m
ORDER BY 
    cohort_index;

SET @columns = LEFT(@columns, LEN(@columns) - 1);

-- Execute dynamic SQL
SET @sql ='
SELECT * FROM     
(
	  SELECT DISTINCT
		Cohort_Date,
		cohort_index,
		CustomerID 
	  FROM #cohort_retention
) t 
PIVOT(
    COUNT(CustomerID) 
    FOR cohort_index IN ('+ @columns +')
) AS pivot_table
ORDER BY Cohort_Date
';

EXECUTE sp_executesql @sql;