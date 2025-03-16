CREATE TABLE date_dim (
    date_id integer PRIMARY KEY,
    decade INT,
    ev_year INT,
    month_int INT,
    month_string varchar(70),
    ev_day INT,
    tem_em_id INT
);

CREATE TABLE location_dim (
    location_id integer PRIMARY KEY,
    continent TEXT,
    continent_id TEXT,
    area TEXT, 
    area_id TEXT, 
    nation TEXT, 
    nation_iso TEXT,
    location_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    tem_em_id INT
);

CREATE TABLE event_type_dim (
    event_type_id integer PRIMARY KEY,
    disaster_group TEXT,
    disaster_subgroup TEXT,
    disaster_type TEXT,
    disaster_subtype TEXT,
    event_name TEXT,
    magnitude double precision,
    magnitude_scale TEXT,
    duration integer,
    response TEXT,
    appeal TEXT,
    declaration TEXT,
    end_date TEXT
);

CREATE TABLE temperature_dim(
    temperature_id integer PRIMARY KEY,
    ev_year INT, -- in common with date dimension
    nation_iso TEXT, -- in common with location dimension
    month_int INT,
    month_string varchar(70),
    temperature_change double precision,
    standard_deviation double precision,
    coal_emission INT,
    oil_emission INT,
    gas_emission INT,
    cement_emission INT,
    flaring_emission INT,
    other_emission INT,
    per_capita_emission INT
);

CREATE TABLE event_fact (
    id SERIAL PRIMARY KEY,
    date_id INT ,
    location_id INT ,
    event_type_id INT ,
    total_deaths BIGINT,  
    num_injured BIGINT,   
    num_affected BIGINT,  
    num_homeless BIGINT,  
    total_affected BIGINT,
    aid_contribution BIGINT, 
    reconstruction_cost BIGINT, 
    reconstruction_cost_adjusted BIGINT,
    insured_damage BIGINT, 
    insured_damage_adjusted BIGINT, 
    total_damage BIGINT, 
    total_damage_adjusted BIGINT,
    FOREIGN KEY (event_type_id) REFERENCES event_type_dim(event_type_id),
    FOREIGN KEY (date_id) REFERENCES date_dim(date_id),
    FOREIGN KEY (location_id) REFERENCES location_dim(location_id)
);

