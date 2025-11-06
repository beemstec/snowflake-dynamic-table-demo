-- ==========================================================
-- PROJECT: Employee Data Pipeline using Dynamic Tables
-- ARCHITECTURE: Raw → Bronze → Silver → Gold (ELT Pattern)
-- PURPOSE: Demonstrate how Dynamic Tables automate data refresh
-- ==========================================================


-- ==========================================================
-- Step 1: Create Database and Layered Schemas
-- ==========================================================
CREATE OR REPLACE DATABASE demo_emp_db;

-- Raw:   Landing zone for unprocessed data (direct ingestion)
CREATE OR REPLACE SCHEMA demo_emp_db.emp_raw;

-- Bronze: Minimal cleanup (basic structure alignment)
CREATE OR REPLACE SCHEMA demo_emp_db.emp_bronze;

-- Silver: Standardized, cleaned, enriched data
CREATE OR REPLACE SCHEMA demo_emp_db.emp_silver;

-- Gold:   Aggregated and business-ready data for reporting
CREATE OR REPLACE SCHEMA demo_emp_db.emp_gold;



-- ==========================================================
-- Step 2: RAW LAYER – Source Table
-- ==========================================================
USE SCHEMA demo_emp_db.emp_raw;

-- Create the raw employees table (no transformations)
CREATE OR REPLACE TABLE raw_employees (
    employee_id     INT,
    name            STRING,
    department      STRING,
    salary          NUMBER(10,2),
    joining_date    DATE,
    location        STRING
);

-- Insert sample raw data
INSERT INTO raw_employees VALUES
(101, 'alice stewart', 'Finance', 75000, '2022-01-15', 'Bangalore'),
(102, 'bob', 'IT', 90000, '2021-07-20', 'Hyderabad'),
(103, 'charlie', 'HR', 65000, '2023-03-10', 'Chennai'),
(104, 'Bob 104', 'IT IT', 90000, '2021-07-20', 'Hyderabad');

-- View raw data
SELECT * FROM raw_employees;



-- ==========================================================
-- Step 3: BRONZE LAYER – Dynamic Table (Landing Transform)
-- ==========================================================
USE SCHEMA demo_emp_db.emp_bronze;

-- Bronze layer stores cleansed version of raw data
CREATE OR REPLACE DYNAMIC TABLE bronze_employees
    LAG = '5 MINUTE'                -- Refresh frequency
    WAREHOUSE = etl_wh
AS
SELECT
    employee_id,
    TRIM(name) AS name,
    TRIM(department) AS department,
    salary,
    joining_date,
    TRIM(location) AS location
FROM demo_emp_db.emp_raw.raw_employees;

-- Force initial refresh
ALTER DYNAMIC TABLE demo_emp_db.emp_bronze.bronze_employees REFRESH;



-- ==========================================================
-- Step 4: SILVER LAYER – Dynamic Table (Standardization)
-- ==========================================================
USE SCHEMA demo_emp_db.emp_silver;

CREATE OR REPLACE DYNAMIC TABLE silver_employees
    LAG = '5 MINUTE'
    WAREHOUSE = etl_wh
AS
SELECT
    employee_id,
    INITCAP(name) AS name,              -- Proper casing for names
    UPPER(department) AS department,    -- Department in uppercase
    salary,
    joining_date,
    location
FROM demo_emp_db.emp_bronze.bronze_employees;

-- Force initial refresh
ALTER DYNAMIC TABLE demo_emp_db.emp_silver.silver_employees REFRESH;



-- ==========================================================
-- Step 5: GOLD LAYER – Dynamic Table (Aggregated Business View)
-- ==========================================================
USE SCHEMA demo_emp_db.emp_gold;

CREATE OR REPLACE DYNAMIC TABLE gold_employees_summary
    LAG = '10 MINUTE'
    WAREHOUSE = etl_wh
AS
SELECT
    department,
    COUNT(*) AS total_employees,
    ROUND(AVG(salary), 2) AS avg_salary
FROM demo_emp_db.emp_silver.silver_employees
GROUP BY department;

-- Force initial refresh
ALTER DYNAMIC TABLE demo_emp_db.emp_gold.gold_employees_summary REFRESH;



-- ==========================================================
-- Step 6: VALIDATION – View Layer Outputs
-- ==========================================================
-- You can validate how data moves from raw to gold

-- Raw (Landing)
SELECT * FROM demo_emp_db.emp_raw.raw_employees;

-- Bronze (Cleaned, structural consistency)
SELECT * FROM demo_emp_db.emp_bronze.bronze_employees;

-- Silver (Standardized names and departments)
SELECT * FROM demo_emp_db.emp_silver.silver_employees;

-- Gold (Aggregated business summary)
SELECT * FROM demo_emp_db.emp_gold.gold_employees_summary;



-- ==========================================================
-- Step 7: MONITORING – Dynamic Table Status
-- ==========================================================
-- View list of all Dynamic Tables in this database
SHOW DYNAMIC TABLES IN DATABASE demo_emp_db;

-- Check refresh history (useful for debugging or scheduling)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY START_TIME DESC;
