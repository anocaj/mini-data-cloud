-- Iceberg Catalog Database Initialization Script
-- This script is executed when the PostgreSQL container starts for the first time

-- Create the catalog database schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS iceberg_catalog;

-- Set the search path to include the iceberg catalog schema
SET search_path TO iceberg_catalog, public;

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

-- Create additional tables for enhanced metadata management
CREATE TABLE IF NOT EXISTS iceberg_catalog.table_statistics (
    table_identifier VARCHAR(255) PRIMARY KEY,
    row_count BIGINT,
    data_size BIGINT,
    file_count INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS iceberg_catalog.partition_statistics (
    table_identifier VARCHAR(255),
    partition_spec TEXT,
    row_count BIGINT,
    data_size BIGINT,
    file_count INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_identifier, partition_spec)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_table_stats_updated ON iceberg_catalog.table_statistics(last_updated);
CREATE INDEX IF NOT EXISTS idx_partition_stats_updated ON iceberg_catalog.partition_statistics(last_updated);

-- Grant permissions on the new tables
GRANT ALL PRIVILEGES ON iceberg_catalog.table_statistics TO minicloud;
GRANT ALL PRIVILEGES ON iceberg_catalog.partition_statistics TO minicloud;

-- Log successful initialization
INSERT INTO iceberg_catalog.table_statistics (table_identifier, row_count, data_size, file_count) 
VALUES ('_catalog_init_marker', 0, 0, 0) 
ON CONFLICT (table_identifier) DO NOTHING;