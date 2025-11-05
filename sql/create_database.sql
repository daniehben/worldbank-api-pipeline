DROP DATABASE IF EXISTS mena_gender_sql;
CREATE DATABASE mena_gender_sql;
USE mena_gender_sql;

CREATE TABLE gender_data (
    year INT,
    value FLOAT,
    country_id VARCHAR(10),
    country_name VARCHAR(100),
    indicator_id VARCHAR(50),
    indicator_name VARCHAR(255),
    unit_type VARCHAR(50)
);

