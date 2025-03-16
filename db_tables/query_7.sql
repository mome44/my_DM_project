/*Comparison of average deaths by disaster subgroup in 1960s vs 2010s*/
select e.disaster_subgroup, d.decade, avg(f.total_deaths) as avg_deaths
from event_fact f, event_type_dim e, date_dim d
where d.decade in ('1960', '2010') and f.event_type_id = e.event_type_id and f.date_id = d.date_id
group by e.disaster_subgroup, d.decade
order by e.disaster_subgroup, d.decade, avg_deaths desc;