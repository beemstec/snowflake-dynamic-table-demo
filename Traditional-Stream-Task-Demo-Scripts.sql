-- ==========================================================
-- PROJECT: Employee Data Pipeline using Streams & Tasks
-- ARCHITECTURE: Raw → Bronze → Silver → Gold
-- PURPOSE: Demonstrate Continuous ELT using Streams and Tasks
-- ==========================================================


-- ==========================================================
-- Step 1: Set Up Database and Schemas
-- ==========================================================

-- Create a dedicated database for demo
CREATE OR REPLACE DATABASE demo_emp_db;

-- Layered schemas (each represents a stage in the pipeline)
CREATE OR REPLACE SCHEMA demo_emp_db.emp_raw;      -- Raw / Landing data
CREATE OR REPLACE SCHEMA demo_emp_db.emp_bronze;   -- Cleansed / Base layer
CREATE OR REPLACE SCHEMA demo_emp_db.emp_silver;   -- Transformed / Standardized
CREATE OR REPLACE SCHEMA demo_emp_db.emp_gold;     -- Aggregated / Business insights


-- ==========================================================
-- Step 2: RAW LAYER — Source Table (Manual Inserts)
-- ==========================================================
USE SCHEMA demo_emp_db.emp_raw;

-- Define raw employee data table
CREATE OR REPLACE TABLE raw_employees (
    employee_id     INT,
    name            STRING,
    department      STRING,
    salary          NUMBER(10,2),
    joining_date    DATE,
    location        STRING
);

-- Insert sample records (simulating incoming data)
INSERT INTO raw_employees VALUES
(101, 'Alice',   'Finance', 75000, '2022-01-15', 'Bangalore'),
(102, 'Bob',     'IT',      90000, '2021-07-20', 'Hyderabad'),
(103, 'Charlie', 'HR',      65000, '2023-03-10', 'Chennai'),
(104, 'Bob 104', 'IT IT',   90000, '2021-07-20', 'Hyderabad');

-- Validate raw data
SELECT * FROM raw_employees;



-- ==========================================================
-- Step 3: BRONZE LAYER — Base Cleaned Data + Stream Setup
-- ==========================================================
USE SCHEMA demo_emp_db.emp_bronze;

-- 3.1 Create Bronze Table
-- (A replica of raw data, acts as the base layer for streaming)
CREATE OR REPLACE TABLE bronze_employees AS
SELECT * FROM demo_emp_db.emp_raw.raw_employees;

-- Check loaded data
SELECT * FROM bronze_employees;

-- 3.2 Create a Stream on Raw Table
-- This stream tracks changes (INSERT/UPDATE/DELETE) on raw_employees
CREATE OR REPLACE STREAM bronze_employees_stream 
ON TABLE demo_emp_db.emp_raw.raw_employees
APPEND_ONLY = FALSE;

-- Verify the stream
SHOW STREAMS IN SCHEMA demo_emp_db.emp_bronze;



-- ==========================================================
-- Step 4: SILVER LAYER — Transformation via Task & Stream
-- ==========================================================
USE SCHEMA demo_emp_db.emp_silver;

-- 4.1 Define the target Silver table
-- (This holds transformed, up-to-date data from the bronze stream)
CREATE OR REPLACE TABLE silver_employees (
    employee_id INT,
    name STRING,
    department STRING,
    salary NUMBER(10,2),
    joining_date DATE
);

-- 4.2 Create a Warehouse if not exists (for task execution)
CREATE OR REPLACE WAREHOUSE etl_wh
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

-- 4.3 Create a Task to auto-refresh Silver table using Stream
CREATE OR REPLACE TASK silver_employees_task
WAREHOUSE = etl_wh
SCHEDULE = '5 MINUTE'
COMMENT = 'Merge latest raw changes into silver_employees every 5 minutes'
AS
MERGE INTO silver_employees t
USING (
    SELECT 
        employee_id,
        INITCAP(TRIM(name)) AS name,
        UPPER(TRIM(department)) AS department,
        salary,
        joining_date
    FROM demo_emp_db.emp_bronze.bronze_employees_stream
) s
ON t.employee_id = s.employee_id
WHEN MATCHED THEN UPDATE SET
    t.name = s.name,
    t.department = s.department,
    t.salary = s.salary,
    t.joining_date = s.joining_date
WHEN NOT MATCHED THEN
INSERT (employee_id, name, department, salary, joining_date)
VALUES (s.employee_id, s.name, s.department, s.salary, s.joining_date);

-- 4.4 Start the task (activate scheduling)
ALTER TASK silver_employees_task RESUME;

-- Check stream content
SELECT * FROM demo_emp_db.emp_bronze.bronze_employees_stream;

-- Validate Silver table
SELECT * FROM silver_employees;



-- ==========================================================
-- Step 5: GOLD LAYER — Aggregation using Task
-- ==========================================================
USE SCHEMA demo_emp_db.emp_gold;

-- 5.1 Create target summary table
CREATE OR REPLACE TABLE gold_employees_summary (
    department STRING,
    total_employees INT,
    avg_salary NUMBER(10,2)
);

-- 5.2 Create Task to refresh Gold summary every 10 minutes
CREATE OR REPLACE TASK demo_emp_db.emp_gold.gold_employees_summary_task
WAREHOUSE = etl_wh
SCHEDULE = '10 MINUTE'
COMMENT = 'Refresh employee summary (department-wise) every 10 minutes'
AS
CREATE OR REPLACE TABLE demo_emp_db.emp_gold.gold_employees_summary AS
SELECT 
    department,
    COUNT(*) AS total_employees,
    ROUND(AVG(salary), 2) AS avg_salary
FROM demo_emp_db.emp_silver.silver_employees
GROUP BY department;

-- Start the gold task
ALTER TASK demo_emp_db.emp_gold.gold_employees_summary_task RESUME;



-- ==========================================================
-- Step 6: VALIDATION — Check Each Layer
-- ==========================================================

-- Raw (landing)
SELECT * FROM demo_emp_db.emp_raw.raw_employees;

-- Bronze (mirror + stream)
SELECT * FROM demo_emp_db.emp_bronze.bronze_employees;
SELECT * FROM demo_emp_db.emp_bronze.bronze_employees_stream;

-- Silver (transformed)
SELECT * FROM demo_emp_db.emp_silver.silver_employees;

-- Gold (aggregated summary)
SELECT * FROM demo_emp_db.emp_gold.gold_employees_summary;



-- ==========================================================
-- Step 7: MONITOR TASKS & STREAMS
-- ==========================================================
SHOW STREAMS IN DATABASE demo_emp_db;
SHOW TASKS IN DATABASE demo_emp_db;

-- Check stream offsets and lag
SELECT * FROM TABLE(INFORMATION_SCHEMA.STREAMS());

-- Task execution history (for troubleshooting)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC;
