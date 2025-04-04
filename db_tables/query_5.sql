--Top 10 most affected nations by disaster type
select l.nation, e.disaster_type, sum(f.total_affected) as total_affected

from event_fact f, location_dim l, event_type_dim e

where f.location_id = l.location_id and f.event_type_id = e.event_type_id

group by l.nation, e.disaster_type

order by total_affected desc

limit 10;