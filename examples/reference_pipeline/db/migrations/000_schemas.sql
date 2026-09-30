-- 000_schemas.sql
-- Layer schemas for the reference HR pipeline (example pipeline under test).
-- Layers: bronze (raw landing), silver (curated, SCD Type 4),
--         gold (consumption), ops (ingestion registry + audit).
-- This is example DDL, not part of the test framework.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;
