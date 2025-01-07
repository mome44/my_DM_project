--Seasonality of Disasters
SELECT 
    d.month_string, 
    e.disaster_type, 
    COUNT(*) AS disaster_count
FROM 
    event_fact f
JOIN 
    date_dim d ON f.date_id = d.date_id
JOIN 
    event_type_dim e ON f.event_type_id = e.event_type_id
GROUP BY 
    d.month_string, e.disaster_type
ORDER BY 
    d.month_string, disaster_count DESC;