--correlation Between average emissions and event total damage for each year
select t.ev_year as years, avg(t.coal_emission + t.oil_emission + t.gas_emission + t.cement_emission + t.flaring_emission + t.other_emission) as avg_emissions, 
    sum(f.total_damage_adjusted) as total_damage
from event_fact f, date_dim d, temperature_dim  t
where f.date_id = d.date_id and d.ev_year=t.ev_year and t.month_int=d.month_int
group by t.ev_year
order by t.ev_year;