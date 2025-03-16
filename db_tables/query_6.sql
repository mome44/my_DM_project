--Frequency of disaster according to months
select d.month_string, e.disaster_type, count(*) as disaster_count
from event_fact f, date_dim d, event_type_dim e
where f.date_id = d.date_id and f.event_type_id = e.event_type_id
group by d.month_string, e.disaster_type
order by d.month_string, disaster_count desc;