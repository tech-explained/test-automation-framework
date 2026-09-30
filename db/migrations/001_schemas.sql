-- 001_schemas.sql
-- Extensions and layer schemas for the HR Dataflow -> Postgres platform.
-- Layers: bronze (raw landing), silver (curated, SCD Type 4), gold (consumption),
--         ops (ingestion registry + audit), tf (test-framework metadata).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS tf;
