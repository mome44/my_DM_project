--Most Affected Nations by Disaster Type
SELECT 
    l.nation, 
    e.disaster_type, 
    SUM(f.total_affected) AS total_affected
FROM 
    event_fact f
JOIN 
    location_dim l ON f.location_id = l.location_id
JOIN 
    event_type_dim e ON f.event_type_id = e.event_type_id
GROUP BY 
    l.nation, e.disaster_type
ORDER BY 
    total_affected DESC
LIMIT 10;