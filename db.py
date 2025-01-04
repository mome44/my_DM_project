import psycopg2
from sqlalchemy import create_engine,Table, MetaData
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(current_dir, 'db_tables')


def insert_time_dimension(table, connection):
    date_dim = table[['ev_year', 'month_int', 'month_string', 'ev_day']].drop_duplicates()
    #Create decades column
    date_dim['decade'] = (date_dim['ev_year'] // 10) * 10
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

def insert_data(table, connection, target_table, columns):
  
    # Select and rename columns based on mappings
    data_to_insert = table[columns].drop_duplicates()

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

def insert_space_dimension(table, connection):
    return
def insert_event_dimension(table, connection):
    return
def insert_temperature_dimension(table, connection):
    return
def insert_event_fact(table, connection):
    return


def is_numeric_column(column):
    try:
        pd.to_numeric(column)
        return True
    except ValueError:
        return False

if __name__=='__main__':
    event_table= pd.read_csv('cleaned_datasets/cleaned_event_econ.csv', encoding='latin1',delimiter=';')
    temperature_table = pd.read_csv('cleaned_datasets/cleaned_temperature.csv', encoding='latin1',delimiter=';')
    emission_table= pd.read_csv('cleaned_datasets/cleaned_emission_1.csv', encoding='latin1',delimiter=';')

    print(event_table.columns)
    print(temperature_table.columns)
    
    whole_table = pd.merge(event_table, temperature_table, on=['ev_year', 'nation','month_int', 'month_string'], how='inner')
    whole_table = pd.merge(whole_table, emission_table, on=['ev_year','nation', 'nation_iso'], how='inner')

    whole_table.to_csv('cleaned_datasets/whole.csv', sep=';', decimal=',', index=False)
    numeric_columns=['response', 'appeal', 'declaration',
       'aid_contribution', 'magnitude', 'latitude',
       'longitude', 'total_deaths',
       'num_injured', 'num_affected', 'num_homeless', 'total_affected',
       'reconstruction_cost', 'reconstruction_cost_adjusted', 'insured_damage',
       'insured_damage_adjusted', 'total_damage_adjusted', 'end_date',
       'duration', 'month_string', 'total_damage', 'standard_deviation',
       'temperature_change', 'total', 'coal_emission', 'oil_emission',
       'gas_emission', 'cement_emission', 'flaring_emission', 'other_emission',
       'per_capita_emission', 'continent_code', 'sub-continent_code']
    
    for c in numeric_columns:
       whole_table[c] = pd.to_numeric(whole_table[c].astype(str).str.replace(',', '.'), errors='coerce')

    whole_table.to_csv('cleaned_datasets/whole_2.csv', sep=';', decimal=',', index=False)
    #Adding the misssing columns 
    Un_geographic_data= pd.read_csv('utils/UNSD_Methodology.csv', encoding='latin1',delimiter=';')
    Un_geographic_data=Un_geographic_data[['Global Name','Region Code', 'Region Name', 'Sub-region Code', 'Sub-region Name', 'ISO-alpha3 Code']]
    print(Un_geographic_data)

    Un_geographic_data.columns=Un_geographic_data.columns.str.lower().str.replace(' ', '_').str.replace('region', 'continent')
    Un_geographic_data=Un_geographic_data.rename(columns={'iso-alpha3_code':'nation_iso'})
    Un_geographic_data['nation_iso']= Un_geographic_data['nation_iso'].str.lower()
    whole_table['nation_iso']=whole_table['nation_iso'].astype(str)
    Un_geographic_data['nation_iso']=Un_geographic_data['nation_iso'].astype(str)
    
    #whole_table = pd.merge(whole_table, Un_geographic_data, on=['nation_iso'], how='inner')
    #whole_table.to_csv('cleaned_datasets/whole_2.csv', sep=';', decimal=',', index=False)



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
    
  
    

    col_date=['ev_year', 'month_int', 'month_string', 'ev_day']

    insert_data(whole_table, connection, 'date_dim', col_date)

    col_space=["nation", "nation_iso", "region", "subregion", 
    "location_name", "latitude", "longitude"]

    insert_data(whole_table, connection, 'location_dim', col_space)

    #location_dim_columns = ["location_id", "continent", "continent_code", 
    #"area", "area_code", "nation", "nation_iso", "subregion", 
    #"location_name", "city", "latitude", "longitude", 
    #"tem_em_id"]

    event_type_dim_columns = [
    "disaster_group", "disaster_subgroup", 
    "disaster_type", "disaster_subtype", "event_name", 
    "magnitude", "magnitude_scale", "duration", "response", 
    "appeal", "declaration", "end_date"
    ]

    insert_data(whole_table, connection, 'event_type_dim', event_type_dim_columns)

    # Column names for temperature_dim
    temperature_dim_columns = [
        "ev_year", "nation_iso", "temperature_change", 
        "standard_deviation", "coal_emission", "oil_emission", 
        "gas_emission", "cement_emission", "flaring_emission", 
        "other_emission", "per_capita_emission"
    ]

    insert_data(whole_table, connection, 'temperature_dim', temperature_dim_columns)

    # Column names for event_fact
    event_fact_columns = [
        "total_deaths", "num_injured", "num_affected", 
        "num_homeless", "total_affected", "aid_contribution", 
        "reconstruction_cost", "reconstruction_cost_adjusted", 
        "insured_damage", "insured_damage_adjusted", 
        "total_damage", "total_damage_adjusted"
    ]

    insert_data(whole_table, connection, 'event_fact', event_fact_columns)

    connection.close()

    #insert_space_dimension(whole_table)
#
    #insert_event_dimension(whole_table)
#
    #insert_temperature_dimension(whole_table)
#
    #insert_event_fact(whole_table)
    ## Extract the date-related columns
    #date_dim = whole_table[['ev_year', 'month_int', 'month_string', 'ev_day']].drop_duplicates()
    #date_dim['decade'] = (date_dim['ev_year'] // 10) * 10
#
    #
#
    ## Create a unique 'date_id' for the dimension
    #date_dim['date_id'] = date_dim.index + 1
#
    ## Reorder the columns to match the schema
    #date_dim = date_dim[['date_id', 'decade', 'ev_year', 'month_int', 'month_string', 'ev_day', 'tem_em_id']]

    ## Check the result
    #print(date_dim.head())

    ## Extract the event-related columns
    #event_type_dim = whole_table[['disaster_group', 'disaster_subgroup', 'disaster_type', 'disaster_subtype', 'event_name', 'magnitude', 'magnitude_scale', 'duration', 'response', 'appeal', 'declaration', 'end_date']].drop_duplicates()

    ## Add a unique 'event_type_id' for the dimension
    #event_type_dim['event_type_id'] = event_type_dim.index + 1
#
    ## Check the result
    #print(event_type_dim.head())
#

    ## Extract the temperature-related columns
    #temperature_dim = whole_table[['ev_year', 'nation_iso', 'temperature_change', 'standard_deviation', 'coal_emission', 'oil_emission', 'gas_emission', 'cement_emission', 'flaring_emission', 'other_emission', 'per_capita_emission']].drop_duplicates()

    ## Add a unique 'id' for the dimension
    #temperature_dim['id'] = temperature_dim.index + 1

    ## Add foreign keys to the date_dim and location_dim
    #temperature_dim['year_name'] = temperature_dim['ev_year']
    #temperature_dim['nation_iso'] = temperature_dim['nation_iso']
#
    ## Check the result
    #print(temperature_dim.head())
#
    ## Merge the necessary dimensions to create the fact table
    #event_fact = whole_table[['total_deaths', 'num_injured', 'num_affected', 'num_homeless', 'total_affected', 'aid_contribution', 
    #                 'reconstruction_cost', 'reconstruction_cost_adjusted', 'insured_damage', 'insured_damage_adjusted', 
    #                 'total_damage', 'total_damage_adjusted']]
#
    ## Add foreign keys to the fact table by merging with the dimensions
    #event_fact = event_fact.merge(date_dim[['date_id', 'ev_year', 'month_int', 'ev_day']], left_on=['ev_year', 'month_int', 'ev_day'], right_on=['ev_year', 'month_int', 'ev_day'], how='left')
    #event_fact = event_fact.merge(location_dim[['location_id', 'nation_iso', 'location_name']], left_on=['nation_iso', 'location_name'], right_on=['nation_iso', 'location_name'], how='left')
    #event_fact = event_fact.merge(event_type_dim[['event_type_id', 'disaster_group', 'disaster_subgroup', 'disaster_type']], left_on=['disaster_group', 'disaster_subgroup', 'disaster_type'], right_on=['disaster_group', 'disaster_subgroup', 'disaster_type'], how='left')
#
    ## Check the result
    #print(event_fact.head())
#
#
#
    ## Create the connection to your database
    #engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
#
    ## Insert the DataFrames into their respective tables
    #date_dim.to_sql('date_dim', engine, if_exists='replace', index=False)
    #location_dim.to_sql('location_dim', engine, if_exists='replace', index=False)
    #event_type_dim.to_sql('event_type_dim', engine, if_exists='replace', index=False)
    #temperature_dim.to_sql('temperature_dim', engine, if_exists='replace', index=False)
    #event_fact.to_sql('event_fact', engine, if_exists='replace', index=False)




    