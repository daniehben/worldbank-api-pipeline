USE mena_gender_db;


SELECT COUNT(*) AS total_rows FROM gender_data_raw;
SELECT COUNT(DISTINCT country_id) AS countries,
       COUNT(DISTINCT indicator_id) AS indicators,
       COUNT(DISTINCT year) AS years
FROM gender_data_raw;


SELECT * FROM gender_data_raw LIMIT 10;
