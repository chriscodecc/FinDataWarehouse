-- Unternehmen
CREATE TABLE IF NOT EXISTS dim_company (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    symbol VARCHAR(20) UNIQUE NOT NULL, -- Wichtig für den Join im Python-Skript
    country VARCHAR(100),
    industry VARCHAR(255)
);

-- Datum
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY, -- Format YYYYMMDD 
    full_date DATE UNIQUE NOT NULL,
    day INT,
    month INT,
    year INT
);

-- Staging-Tabelle (Rohdaten-Zwischenspeicher)
CREATE TABLE IF NOT EXISTS stg_prices (
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    asset VARCHAR(20),
    full_date DATE,
    open_price NUMERIC(20, 6),
    high_price NUMERIC(20, 6),
    low_price NUMERIC(20, 6),
    close_price NUMERIC(20, 6),
    volume BIGINT
);

-- Fact-Tabelle 
CREATE TABLE IF NOT EXISTS fact_prices (
    price_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    company_id INT REFERENCES dim_company(company_id),
    close_price NUMERIC(20, 6),
    high_price NUMERIC(20, 6),
    low_price NUMERIC(20, 6),
    open_price NUMERIC(20, 6),
    volume BIGINT,
    UNIQUE (date_id, company_id)
);

