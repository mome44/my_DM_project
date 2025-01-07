--total deaths and injuries by disaster group for each decade
select d.decade, e.disaster_group, sum(f.total_deaths) as total_deaths,
	sum(f.num_injured) as total_injuries

from event_fact f, date_dim d, event_type_dim e

where f.date_id = d.date_id and 
	f.event_type_id = e.event_type_id

group by d.decade, e.disaster_group
order by d.decade, e.disaster_group;