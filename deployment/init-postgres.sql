-- Create databases for Dagster and mkpipe backends
CREATE DATABASE dagster;
CREATE DATABASE mkpipe;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE dagster TO postgres;
GRANT ALL PRIVILEGES ON DATABASE mkpipe TO postgres;
