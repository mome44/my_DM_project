import pandas as pd
from openpyxl import load_workbook, Workbook
import json


#CONSTANTS
MISSING_STRING='unknown'
MISSING_NUMBER=0
MISSING_STRING_2='Not specified'

def load_location_coords(path):
    with open(path, 'r') as file:
        country_coords = json.load(file)
    return country_coords

def clean_economy(economy):
    print (economy.columns)
    economy_cleaned = economy.drop(columns=['Code'])
    economy_cleaned['Total economic damages'] = pd.to_numeric(economy_cleaned['Total economic damages'], errors='coerce')
    missing_values = economy_cleaned.isnull().sum()
    return economy_cleaned

def clean_event(event):
    for c in event.columns:
        print(pd.api.types.is_numeric_dtype(event[c]), c)

    #DELETING USELESS COLUMNS
    events_cleaned = event.drop(columns=['Historic', 'External IDs', 'Origin', 'Associated Types', 'River Basin','CPI', 'Admin Units', 'Entry Date', 'Last Update'])
    
    #CONVERT COLUMNS TO NUMERIC
    numeric_columns=['Latitude', 'Longitude', 'Magnitude', 'Start Year', 'Start Month','Start Day', 'End Year', 'End Month', 'End Day', 'Total Deaths'
                     ,'No. Injured', 'No. Affected', 'No. Homeless', 'Total Affected', 'AID Contribution (\'000 US$)', 'Total Damage, Adjusted (\'000 US$)'
                     ,'Total Damage (\'000 US$)', 'Insured Damage (\'000 US$)','Insured Damage, Adjusted (\'000 US$)', 'Reconstruction Costs (\'000 US$)'
                     , 'Reconstruction Costs, Adjusted (\'000 US$)']
    for col in numeric_columns:
        events_cleaned[col] = pd.to_numeric(events_cleaned[col], errors='coerce')

    events_cleaned['Location'].fillna(MISSING_STRING_2, inplace=True)
    events_cleaned['Location']=events_cleaned['Location'].str.split(',')
    events_cleaned = events_cleaned.explode('Location')
    events_cleaned['Location']=events_cleaned['Location'].str.strip()

    location_coords=load_location_coords('utils/country_coords.json')

    for idx, row in events_cleaned.iterrows():
        if pd.isnull(row['Latitude']) or pd.isnull(row['Longitude']):
            lat, lon = location_coords.get(str(idx))
            print(idx,lat,lon)
            events_cleaned.at[idx, 'Latitude'] = lat
            events_cleaned.at[idx, 'Longitude'] = lon

    events_cleaned=normalize_table(events_cleaned)
    
    return events_cleaned

def clean_temperature(temperature):

    df_melted = pd.melt(temperature, 
                    id_vars=['Area Code', 'Area', 'Months Code', 'Months', 'Element', 'Unit','Element Code'], 
                    var_name='Year', 
                    value_name='Value')

    # Convert the 'Year' column from 'Y1961' to just '1961'
    df_melted['Year'] = df_melted['Year'].str.replace('Y', '').astype(int)

    # Pivot the 'Element' column to create 'Temperature change' and 'Standard deviation' as separate columns
    df_pivoted = df_melted.pivot_table(index=['Area Code', 'Area', 'Months Code', 'Months', 'Year'], 
                                       columns='Element', 
                                       values='Value').reset_index()

    # Rename the columns for clarity
    df_pivoted.columns.name = None
    df_pivoted.rename(columns={'Temperature change': 'Temperature Change', 
                           'Standard deviation': 'Standard Deviation'}, inplace=True)
    cleaned_temperature=df_pivoted
    cleaned_temperature=normalize_table(cleaned_temperature)
    cleaned_temperature=cleaned_temperature.drop(columns=['Area Code', 'Months Code'])
    for column in cleaned_temperature.columns:
        missing_values = cleaned_temperature[column].isnull().sum()
        print(f"Column: {column}, Missing Values: {missing_values}")
    return cleaned_temperature








def normalize_table(table):
    for column in table.columns:
        if table[column].isnull().any():
            if table[column].dtype == 'object':
                table[column].fillna(MISSING_STRING, inplace=True)
                table[column] = table[column].str.lower().str.strip()

            elif pd.api.types.is_numeric_dtype(table[column]):
                table[column].fillna(MISSING_NUMBER, inplace=True)

            elif table[column].dtype == 'bool':
                table[column].fillna(False, inplace=True)
    return table

def clean_emission(emission):
    cleaned_emission=normalize_table(emission)
    return cleaned_emission


if __name__=='__main__':
    economy_table= pd.read_csv('datasets/economic-damage-from-natural-disasters.csv', encoding='latin1')
    event_table= pd.read_excel('datasets/public_emdat_custom_request_2024-06-26_65811dbf-f6a0-43b0-a0af-a151ab0b3ee7.xlsx')
    temperature_table = pd.read_csv('datasets/Environment_Temperature_change_E_All_Data_NOFLAG.csv', encoding='latin1')
    emission_table_1= pd.read_csv('datasets/GCB2022v27_MtCO2_flat.csv', encoding='latin1')
    emission_table_2= pd.read_csv('datasets/GCB2022v27_percapita_flat.csv', encoding='latin1')
    emission_table_3 = pd.read_csv('datasets/GCB2022v27_sources_flat.csv', encoding='latin1')

    ##CLEANING economy table
    print('starting to clean economy table')
    economy_cleaned= clean_economy(economy_table)
    economy_cleaned.to_csv('cleaned_datasets/cleaned_economic_damage.csv', sep=';', decimal=',', index=False)
    print(economy_cleaned.head())

    #CLEANING event table
    print('starting to clean event table')
    event_cleaned=clean_event(event_table)
    event_cleaned.to_csv('cleaned_datasets/cleaned_event.csv', sep=';', decimal=',', index=False)
    print(event_cleaned.head())
    
    #CLEANING temperature table
    print ('starting to clean temperature table')
    temperature_table_cleaned=clean_temperature(temperature_table)
    temperature_table_cleaned.to_csv('cleaned_datasets/cleaned_temperature.csv', sep=';', decimal=',', index=False)
    
