import psycopg2
import pandas as pd
import json

def insert_data(data_to_insert, connection, target_table, columns):
    #This function fills table in postgresql, it takes in input:
    # - the pandas dataframe (data_to_insert)
    # - the connection with the db
    # - the name of the postgres table
    # - the name of the columns of the pg table

    #I initialize the connection cursor
    with connection.cursor() as cursor:
        #Here i join all the columns name separated by a comma
        columns_str = ", ".join(columns) 
        #I create the placeholder for each column
        placeholders = ", ".join(["%s"] * len(columns))
        #I write the query
        insert_query = f"""
            INSERT INTO {target_table} ({columns_str})
            VALUES ({placeholders})
        """
        #I iterate for each row and execute the query
        for _, row in data_to_insert.iterrows():
            cursor.execute(insert_query, tuple(row))
        connection.commit()

    print(f" - Data inserted successfully into {target_table}.")


if __name__=='__main__':
    event_table= pd.read_csv('cleaned_datasets/cleaned_event_econ.csv', encoding='latin1',delimiter=';')
    temperature_table = pd.read_csv('cleaned_datasets/cleaned_temperature.csv', encoding='latin1',delimiter=';')
    emission_table= pd.read_csv('cleaned_datasets/cleaned_emission_1.csv', encoding='latin1',delimiter=';')

    #I merge all the cleaned tables into a single one by making joins on the common attributes
    whole_table = pd.merge(event_table, temperature_table, on=['ev_year', 'nation','month_int', 'month_string'], how='inner')
    whole_table = pd.merge(whole_table, emission_table, on=['ev_year','nation', 'nation_iso'], how='inner')

    #Since when I inserted the tables in sql, it gave me a problem, about float numbers, since
    #postgres wants it separated by a dot, and they were saved with comma.
    numeric_columns=[ 'magnitude', 'latitude',
       'longitude',  'total_damage', "total_deaths",'standard_deviation',
       'num_injured', 'num_affected', 'num_homeless', 'total_affected',
       'reconstruction_cost', 'reconstruction_cost_adjusted', 'insured_damage',
       'insured_damage_adjusted', 'total_damage_adjusted',  "aid_contribution", 
       'temperature_change', 'total', 'coal_emission', 'oil_emission',
       'gas_emission', 'cement_emission', 'flaring_emission', 'other_emission',
       'per_capita_emission']
    
    for c in numeric_columns:
       #I iterate through all the numeric columns and I convert them to string, to replace the character and
       #then back to numeric columns
       whole_table[c] = pd.to_numeric(whole_table[c].astype(str).str.replace(',', '.'), errors='coerce')
    
    #I add the column decade to the table
    whole_table['decade'] = (whole_table['ev_year'] // 10) * 10

    #I save the final version of the table    
    whole_table.to_csv('cleaned_datasets/whole.csv', sep=';', decimal='.', index=False)
    
    #INSERTING THE TABLES IN POSTGRESQL

    #First I open the two files containing the drop table queries and the
    #create table queries
    with open('db_tables/drop_table.sql', "r", encoding="utf-8") as file:
        drop_table = file.read()
    with open('db_tables/create_table.sql', "r", encoding="utf-8") as file:
        create_table = file.read()
    
    with open('utils/config.json', 'r', encoding='utf-8') as file:
        config = json.load(file)
    
    #I try to initialize the psycopg2 connection
    connection = None
    try:
        connection = psycopg2.connect(dbname=config['dbname'],user=config['user'],password=config['password'],host=config['host'],port=config['port'])
        cur = connection.cursor()
        cur.execute(drop_table)
        connection.commit()
        cur.execute(create_table)
        connection.commit()
        cur.close()
        print("Schema created successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    #Then I insert the data into the respective Data Warehouse table
    
    #The following process is the same for all the other dimension tables
    print('Starting to insert data into date dimension')
    #I list all the attributes of the date dimension table
    col_date=['decade','ev_year', 'month_int', 'month_string', 'ev_day']
    #I select only those attributes from the unified table and drop the
    #duplicates
    date_dim= whole_table[col_date].drop_duplicates()
    #Then I inserted the index column, which is sequential based on the 
    #index
    date_dim['date_id']=date_dim.index +1
    #I copy the column list and I append the id column name to the list
    #because the the original column set must be used for the fact table
    col_date_insert=col_date.copy()
    col_date_insert.append('date_id')

    #I call the insert data function and the data is inserted into the db
    insert_data(date_dim, connection, 'date_dim', col_date_insert)

    print('Starting to insert data into location dimension')
    #I do the same process as before with location dimension
    col_space=["nation", "nation_iso", "continent", "continent_id", "area", 
    "area_id","location_name", "latitude", "longitude"]
    
    location_dim= whole_table[col_space].drop_duplicates()
    location_dim['location_id']=location_dim.index +1
    col_space_insert=col_space.copy()
    col_space_insert.append('location_id')
    
    insert_data(location_dim, connection, 'location_dim', col_space_insert)

    print('Starting to insert data into event dimension')

    event_col = ["disaster_group", "disaster_subgroup",  "disaster_type", 
     "disaster_subtype", "event_name", "magnitude", "magnitude_scale", 
     "duration", "response", "appeal", "declaration", "end_date"]

    event_dim= whole_table[event_col].drop_duplicates()
    event_dim['event_type_id']=event_dim.index +1
    event_col_insert=event_col.copy()
    event_col_insert.append('event_type_id')

    insert_data(event_dim, connection, 'event_type_dim', event_col_insert)

    print('Starting to insert data into temperature dimension')

    temperature_col = [ "ev_year", "nation_iso",'month_int', 'month_string', "temperature_change", 
     "standard_deviation", "coal_emission", "oil_emission",  "gas_emission",
     "cement_emission", "flaring_emission", "other_emission", "per_capita_emission"]

    temperature_dim= whole_table[temperature_col].drop_duplicates()
    temperature_dim['temperature_id']=temperature_dim.index +1
    temperature_col_insert=temperature_col.copy()
    temperature_col_insert.append('temperature_id')

    insert_data(temperature_dim, connection, 'temperature_dim', temperature_col_insert)
    
    print('Starting to insert data into the fact table')
    #In this case I always list the columns name, including also the ids from the
    #dimension tables
    event_fact_columns = ["date_id", "location_id","event_type_id", "total_deaths",
     "num_injured", "num_affected", "num_homeless", "total_affected", "aid_contribution", 
     "reconstruction_cost", "reconstruction_cost_adjusted", "insured_damage",
     "insured_damage_adjusted", "total_damage", "total_damage_adjusted"]
    #I merge the dimension tables with the unified table, on the columns that are not the id
    #Then I select only the attributes of the event fact table
    fact_table = whole_table.merge(event_dim, on=event_col, how='left') \
               .merge(location_dim, on=col_space, how='left') \
               .merge(date_dim, on=col_date, how='left') \
               .merge(temperature_dim, on=temperature_col, how='left') \
               [event_fact_columns]
    #I insert the data
    insert_data(fact_table, connection, 'event_fact', event_fact_columns)
    #I close the connection with the db
    connection.close()






    