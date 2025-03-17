-- Create schemas for the ETL process

-- Create the population schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS population;

-- Create the staging schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS staging;

-- Create the OMOP schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS omop;

-- Grant privileges to the current user
GRANT ALL PRIVILEGES ON SCHEMA population TO CURRENT_USER;
GRANT ALL PRIVILEGES ON SCHEMA staging TO CURRENT_USER;
GRANT ALL PRIVILEGES ON SCHEMA omop TO CURRENT_USER;

-- Log the results
SELECT 'Schemas created successfully' AS message;
