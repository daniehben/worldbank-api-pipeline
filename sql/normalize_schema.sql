USE mena_gender_db;

CREATE TABLE countries (
    country_id VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(255)
);

CREATE TABLE indicators (
    indicator_id VARCHAR(50) PRIMARY KEY,
    indicator_name VARCHAR(255),
    unit_type VARCHAR(50)
);


CREATE TABLE gender_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    country_id VARCHAR(10),
    indicator_id VARCHAR(50),
    year INT,
    value FLOAT,
    FOREIGN KEY (country_id) REFERENCES countries(country_id),
    FOREIGN KEY (indicator_id) REFERENCES indicators(indicator_id)
);


-- Populate tables
INSERT IGNORE INTO countries (country_id, country_name)
SELECT DISTINCT country_id, country_name FROM gender_data_raw;

INSERT IGNORE INTO indicators (indicator_id, indicator_name, unit_type)
SELECT DISTINCT indicator_id, indicator_name, unit_type FROM gender_data_raw;

INSERT INTO gender_data (country_id, indicator_id, year, value)
SELECT country_id, indicator_id, year, value FROM gender_data_raw;


