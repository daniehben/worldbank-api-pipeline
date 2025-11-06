DROP DATABASE IF EXISTS mena_gender_sql;
CREATE DATABASE mena_gender_db;
USE mena_gender_db;

CREATE TABLE gender_data_raw(
    country_name VARCHAR(255),
    country_id VARCHAR(10),
    year INT,
    indicator_id VARCHAR(50),
    indicator_name VARCHAR(255),
    value FLOAT,
    unit_type VARCHAR(50)
);

