--Top 10 world regions that received the most aid in the last 20 years
select l.area, sum(f.aid_contribution) as total_aid_contribution

from event_fact f, location_dim l, date_dim d

where f.location_id = l.location_id and f.date_id= d.date_id 
	and d.ev_year>=2004

group by l.area

order by total_aid_contribution desc

limit 10;
