-- 004_gold.sql
-- GOLD = consumption zone. Read-only views over silver; no duplicated state.
-- BI tools / analysts query gold only.

-- Clean dimensional view of the current roster.
CREATE OR REPLACE VIEW gold.vw_worker_dim AS
SELECT
    worker_id,
    employee_id,
    first_name,
    last_name,
    preferred_name,
    email,
    hire_date,
    termination_date,
    worker_status,
    job_profile,
    job_family,
    department,
    department_id,
    location,
    country,
    manager_worker_id,
    cost_center,
    employment_type,
    worker_type,
    time_type,
    compensation_grade,
    annual_salary,
    currency,
    attributes,
    as_of_date,
    valid_from,
    version,
    (CURRENT_DATE - hire_date)          AS tenure_days,
    (worker_status = 'Active')          AS is_active
FROM silver.workers_current;

-- Point-in-time view: every version that ever was current, plus the current one.
-- Query with: WHERE '2026-09-20 12:00+00' BETWEEN valid_from AND COALESCE(valid_to, now())
CREATE OR REPLACE VIEW gold.vw_worker_history AS
SELECT worker_id, employee_id, first_name, last_name, preferred_name, email,
       hire_date, termination_date, worker_status, job_profile, job_family,
       department, department_id, location, country, manager_worker_id,
       cost_center, employment_type, worker_type, time_type,
       compensation_grade, annual_salary, currency, attributes,
       as_of_date, valid_from, valid_to, version,
       superseded_by_file_id, FALSE AS is_current
FROM silver.workers_history
UNION ALL
SELECT worker_id, employee_id, first_name, last_name, preferred_name, email,
       hire_date, termination_date, worker_status, job_profile, job_family,
       department, department_id, location, country, manager_worker_id,
       cost_center, employment_type, worker_type, time_type,
       compensation_grade, annual_salary, currency, attributes,
       as_of_date, valid_from, NULL::timestamptz AS valid_to, version,
       NULL::uuid AS superseded_by_file_id, TRUE AS is_current
FROM silver.workers_current;

-- Headcount aggregates for dashboards.
CREATE OR REPLACE VIEW gold.vw_headcount_by_department AS
SELECT
    department,
    COUNT(*) FILTER (WHERE worker_status = 'Active') AS active_headcount,
    COUNT(*)                                        AS total_headcount,
    COUNT(*) FILTER (WHERE worker_status = 'Terminated') AS terminated_headcount,
    MAX(as_of_date)                                 AS latest_as_of
FROM silver.workers_current
GROUP BY department;

-- Terminations by month for attrition reporting.
CREATE OR REPLACE VIEW gold.vw_terminations_by_month AS
SELECT
    date_trunc('month', termination_date)::date AS termination_month,
    department,
    COUNT(*) AS terminations
FROM silver.workers_current
WHERE termination_date IS NOT NULL
GROUP BY 1, 2;

-- NOTE: gold.vw_missing_from_latest_snapshot lives in 005_ops.sql because it
-- reads ops.file_ingestions, which is created there.
