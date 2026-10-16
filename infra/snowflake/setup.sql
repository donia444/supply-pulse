-- Snowflake setup for the Supply Pulse platform (YOUR personal account).
-- Run once in Snowsight: select all (Ctrl+A) and press Ctrl+Enter ("Run All").
-- Names must match: services/bronze_loader (SUPPLY_PULSE_DB / SUPPLY_PULSE_WH / BRONZE)
-- and dbt/supply_pulse/profiles.yml (SUPPLY_PULSE_DB, schemas SILVER / GOLD / PROD).
USE ROLE SYSADMIN;

-- Small warehouse that suspends itself after 60 seconds to save credits
CREATE WAREHOUSE IF NOT EXISTS SUPPLY_PULSE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS SUPPLY_PULSE_DB;
CREATE SCHEMA IF NOT EXISTS SUPPLY_PULSE_DB.BRONZE;  -- raw Kafka events (bronze_loader) + flat views (scripts/create_bronze_views.py)
CREATE SCHEMA IF NOT EXISTS SUPPLY_PULSE_DB.SILVER;  -- staging + intermediate (dbt)
CREATE SCHEMA IF NOT EXISTS SUPPLY_PULSE_DB.GOLD;    -- dimensions, facts (dbt)
CREATE SCHEMA IF NOT EXISTS SUPPLY_PULSE_DB.PROD;    -- dbt seeds (mapping tables) when the target is "prod"
