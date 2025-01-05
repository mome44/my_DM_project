import psycopg2
from sqlalchemy import create_engine,Table, MetaData
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(current_dir, 'db_tables')


def insert_time_dimension(table, connection):
    date_dim = table[['ev_year', 'month_int', 'month_string', 'ev_day']].drop_duplicates()
    
    with connection.cursor() as cursor:
        for _, row in date_dim.iterrows():
            cursor.execute(
                f"""
                INSERT INTO date_dim (decade, ev_year, month_int, month_string, ev_day)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (row["decade"], row["ev_year"], row["month_int"], row["month_string"], row["ev_day"])
            )
        connection.commit()

    print("Data of time dimension inserted successfully.")
    connection.close()

def insert_data(data_to_insert, connection, target_table, columns):

    with connection.cursor() as cursor:
        # Create the SQL query dynamically
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"""
            INSERT INTO {target_table} ({columns_str})
            VALUES ({placeholders})
        """
        # Insert each row
        for _, row in data_to_insert.iterrows():
            cursor.execute(insert_query, tuple(row))
        connection.commit()

    print(f"Data inserted successfully into {target_table}.")
    #connection.close()


if __name__=='__main__':
    event_table= pd.read_csv('cleaned_datasets/cleaned_event_econ.csv', encoding='latin1',delimiter=';')
    temperature_table = pd.read_csv('cleaned_datasets/cleaned_temperature.csv', encoding='latin1',delimiter=';')
    emission_table= pd.read_csv('cleaned_datasets/cleaned_emission_1.csv', encoding='latin1',delimiter=';')
    
    whole_table = pd.merge(event_table, temperature_table, on=['ev_year', 'nation','month_int', 'month_string'], how='inner')
    whole_table = pd.merge(whole_table, emission_table, on=['ev_year','nation', 'nation_iso'], how='inner')

    #whole_table.to_csv('cleaned_datasets/whole.csv', sep=';', decimal=',', index=False)
    numeric_columns=[ 'magnitude', 'latitude',
       'longitude',  'total_damage', "total_deaths",'standard_deviation',
       'num_injured', 'num_affected', 'num_homeless', 'total_affected',
       'reconstruction_cost', 'reconstruction_cost_adjusted', 'insured_damage',
       'insured_damage_adjusted', 'total_damage_adjusted',  "aid_contribution", 
       'temperature_change', 'total', 'coal_emission', 'oil_emission',
       'gas_emission', 'cement_emission', 'flaring_emission', 'other_emission',
       'per_capita_emission']
    
    for c in numeric_columns:
       whole_table[c] = pd.to_numeric(whole_table[c].astype(str).str.replace(',', '.'), errors='coerce')
    
    whole_table['decade'] = (whole_table['ev_year'] // 10) * 10
    
    whole_table.to_csv('cleaned_datasets/whole.csv', sep=';', decimal=',', index=False)
    
    #Inserting the tables into sql

    with open('db_tables/drop_table.sql', "r", encoding="utf-8") as file:
        drop_table = file.read()
    with open('db_tables/create_table.sql', "r", encoding="utf-8") as file:
        create_table = file.read()
    connection = None

    try:
        connection = psycopg2.connect(dbname="Dm-Project",user='postgres',password="1234",host="localhost",port="5432")
        cur = connection.cursor()
        cur.execute(drop_table)
        connection.commit()
        cur.execute(create_table)
        connection.commit()
        cur.close()
        print("Schema created successfully.")
        #connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    

    col_date=['decade','ev_year', 'month_int', 'month_string', 'ev_day']

    date_dim= whole_table[col_date].drop_duplicates()
    date_dim['date_id']=date_dim.index +1
    col_date_insert=col_date.copy()
    col_date_insert.append('date_id')

    insert_data(date_dim, connection, 'date_dim', col_date_insert)

    

    col_space=["nation", "nation_iso", "continent", "continent_id", "area", 
    "area_id","location_name", "latitude", "longitude"]
    
    location_dim= whole_table[col_space].drop_duplicates()
    location_dim['location_id']=location_dim.index +1
    col_space_insert=col_space.copy()
    col_space_insert.append('location_id')
    
    insert_data(location_dim, connection, 'location_dim', col_space_insert)

    
 
    #"tem_em_id"]

    event_col = [
    "disaster_group", "disaster_subgroup", 
    "disaster_type", "disaster_subtype", "event_name", 
    "magnitude", "magnitude_scale", "duration", "response", 
    "appeal", "declaration", "end_date"
    ]

    event_dim= whole_table[event_col].drop_duplicates()
    event_dim['event_type_id']=event_dim.index +1
    event_col_insert=event_col.copy()
    event_col_insert.append('event_type_id')
    insert_data(event_dim, connection, 'event_type_dim', event_col_insert)
    

    # Column names for temperature_dim
    temperature_col = [
        "ev_year", "nation_iso", "temperature_change", 
        "standard_deviation", "coal_emission", "oil_emission", 
        "gas_emission", "cement_emission", "flaring_emission", 
        "other_emission", "per_capita_emission"
    ]

    temperature_dim= whole_table[temperature_col].drop_duplicates()
    temperature_dim['temperature_id']=temperature_dim.index +1
    temperature_col_insert=temperature_col.copy()
    temperature_col_insert.append('temperature_id')

    insert_data(temperature_dim, connection, 'temperature_dim', temperature_col_insert)
    
    # Column names for event_fact
    event_fact_columns = [
        "date_id", "location_id","event_type_id",
        "total_deaths", "num_injured", "num_affected", 
        "num_homeless", "total_affected", "aid_contribution", 
        "reconstruction_cost", "reconstruction_cost_adjusted", 
        "insured_damage", "insured_damage_adjusted", 
        "total_damage", "total_damage_adjusted"
    ]


    fact_table = whole_table.merge(event_dim, on=event_col, how='left') \
               .merge(location_dim, on=col_space, how='left') \
               .merge(date_dim, on=col_date, how='left') \
               .merge(temperature_dim, on=temperature_col, how='left') \
               [event_fact_columns]
    
    insert_data(fact_table, connection, 'event_fact', event_fact_columns)

    connection.close()






    