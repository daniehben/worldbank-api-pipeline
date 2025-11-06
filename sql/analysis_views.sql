USE mena_gender_db;

CREATE OR REPLACE VIEW gender_dashboard_view AS
SELECT 
    g.year,
    g.value,
    c.country_name,
    i.indicator_name,
    i.unit_type
FROM gender_data g
JOIN countries c ON g.country_id = c.country_id
JOIN indicators i ON g.indicator_id = i.indicator_id;


CREATE OR REPLACE VIEW avg_indicator_values AS
SELECT 
    c.country_name,
    i.indicator_name,
    ROUND(AVG(g.value), 2) AS avg_value
FROM gender_data g
JOIN countries c ON g.country_id = c.country_id
JOIN indicators i ON g.indicator_id = i.indicator_id
GROUP BY c.country_name, i.indicator_name;