--Most Frequent Disaster Types by Continent
select l.continent, e.disaster_subgroup, count(*) as disaster_count

from event_fact f, location_dim l, event_type_dim e

where f.location_id = l.location_id and 
	f.event_type_id = e.event_type_id

group by l.continent, e.disaster_subgroup

order by l.continent,disaster_count desc;