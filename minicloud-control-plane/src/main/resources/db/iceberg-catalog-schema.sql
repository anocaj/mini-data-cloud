-- Iceberg Catalog Database Schema
-- This script creates the necessary tables for Apache Iceberg JDBC catalog

-- Create the catalog database schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS iceberg_catalog;

-- Set the search path to include the iceberg catalog schema
SET search_path TO iceberg_catalog, public;

-- The JDBC catalog will automatically create these tables when first used:
-- - iceberg_tables: Stores table metadata
-- - iceberg_namespace_properties: Stores namespace properties
-- - iceberg_table_properties: Stores table properties

-- Create indexes for better performance (these will be created automatically by Iceberg)
-- but we can prepare the schema structure

-- Ensure the database has the necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create a function to update timestamps
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Grant necessary permissions to the minicloud user
GRANT ALL PRIVILEGES ON SCHEMA iceberg_catalog TO minicloud;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iceberg_catalog TO minicloud;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA iceberg_catalog TO minicloud;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA iceberg_catalog GRANT ALL ON TABLES TO minicloud;
ALTER DEFAULT PRIVILEGES IN SCHEMA iceberg_catalog GRANT ALL ON SEQUENCES TO minicloud;

-- Create a view to monitor catalog health
CREATE OR REPLACE VIEW catalog_health AS
SELECT 
    'iceberg_catalog' as schema_name,
    current_timestamp as check_time,
    'ready' as status;

COMMENT ON SCHEMA iceberg_catalog IS 'Apache Iceberg catalog metadata storage';