-- Select the Role and Warehouse to create objects
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;



-- 1. Create Database
CREATE DATABASE IF NOT EXISTS STOCK_DB;
USE DATABASE STOCK_DB;



-- 2. Create Schemas
CREATE SCHEMA IF NOT EXISTS RAW;      -- For raw JSON data
CREATE SCHEMA IF NOT EXISTS STAGING;  -- For cleaned data
CREATE SCHEMA IF NOT EXISTS ANALYTICS; -- For final tables



-- 3. Create Internal Stage and a file-format (Your "Free S3"). This is where we will upload files from your laptop
CREATE OR REPLACE FILE FORMAT PUBLIC.JSON_FORMAT TYPE = 'JSON';
CREATE STAGE RAW.LOCAL_STAGE FILE_FORMAT = JSON_FORMAT;



-- 4. Create Raw Tables
-- Recreate table with columns matching the JSON keys
CREATE OR REPLACE TABLE RAW.STOCK_PRICES_JSON (
    TICKER STRING,
    PRICE FLOAT,
    TIMESTAMP STRING,
    -- This column will still auto-populate because it has a default value
    INGESTION_TIME TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.COMPANY_INFO_JSON (
    TICKER STRING,
    NAME STRING,
    SECTOR STRING,
    INDUSTRY STRING,
    MARKET_CAP NUMBER,
    INGESTION_TIME TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);