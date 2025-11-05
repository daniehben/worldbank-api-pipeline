SET GLOBAL local_infile = 1;
USE mena_gender_sql;

LOAD DATA LOCAL INFILE '/Users/daniehbenotman/Desktop/worldbank_tableau_project/data/All_countries_cleaned.csv'
INTO TABLE gender_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(country_name,country_id,year,indicator_id,indicator_name,value,unit_type);


