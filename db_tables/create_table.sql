CREATE TABLE date_dim (
    date_id SERIAL PRIMARY KEY,
    decade INT,
    ev_year INT,
    month_int INT,
    month_string varchar(70),
    ev_day INT,
    tem_em_id INT
);

CREATE TABLE location_dim (
    location_id SERIAL PRIMARY KEY,
    continent TEXT,
    continent_code TEXT,
    area TEXT, 
    area_code TEXT, 
    nation TEXT, 
    nation_iso TEXT,
    region TEXT,
    subregion TEXT,
    location_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    tem_em_id INT
);

CREATE TABLE event_type_dim (
    event_type_id SERIAL PRIMARY KEY,
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
    id SERIAL PRIMARY KEY,
    ev_year INT, 
    nation_iso TEXT, 
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
    total_deaths BIGINT,  -- Change INT to BIGINT if necessary
    num_injured BIGINT,   -- Change INT to BIGINT if necessary
    num_affected BIGINT,  -- Change INT to BIGINT if necessary
    num_homeless BIGINT,  -- Change INT to BIGINT if necessary
    total_affected BIGINT,-- Change INT to BIGINT if necessary
    aid_contribution BIGINT, -- Change INT to BIGINT if necessary
    reconstruction_cost BIGINT, -- Change INT to BIGINT if necessary
    reconstruction_cost_adjusted BIGINT, -- Change INT to BIGINT if necessary
    insured_damage BIGINT, -- Change INT to BIGINT if necessary
    insured_damage_adjusted BIGINT, -- Change INT to BIGINT if necessary
    total_damage BIGINT, -- Change INT to BIGINT if necessary
    total_damage_adjusted BIGINT -- Change INT to BIGINT if necessary
);


-- REFERENCES event_type_dim(event_type_id),