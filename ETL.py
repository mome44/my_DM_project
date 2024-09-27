import pandas as pd
from openpyxl import load_workbook, Workbook
from geopy.geocoders import Nominatim
from time import sleep
geolocator = Nominatim(user_agent="geoapi")

def get_lat_long(location_name):
    try:
        location = geolocator.geocode(location_name, timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except Exception as e:
        print(f"Error fetching location for {location_name}: {e}")
        return None, None

#temperature_table = pd.read_csv('Environment_Temperature_change_E_All_Data_NOFLAG.csv', encoding='latin1')
#emission_table_1= pd.read_csv('GCB2022v27_MtCO2_flat.csv', encoding='latin1')
#emission_table_2= pd.read_csv('GCB2022v27_percapita_flat.csv', encoding='latin1')
#emission_table_3 = pd.read_csv('GCB2022v27_sources_flat.csv', encoding='latin1')
#
#events=load_workbook('public_emdat_custom_request_2024-06-26_65811dbf-f6a0-43b0-a0af-a151ab0b3ee7.xlsx').active
#print (temperature_table.columns)
#print (emission_table_1.columns)
#print (emission_table_2.columns)
#print (emission_table_3.columns)

#temp_cols=temperature_table.columns.to_list()
#emiss_cols=emission_table_1.columns.to_list()
#emiss_cols_2=emission_table_2.columns.to_list()

#for i in range(len(events)):
 #   print(events[i][0].value)

MISSING_STRING ='Unknown'
MISSING_STRING_2 ='Not specified'
MISSING_NUMBER = 'Not_related'

#ECONOMY TABLE -- CLEANING
economy= pd.read_csv('economic-damage-from-natural-disasters.csv', encoding='latin1')
print (economy.columns)
# Drop the 'Code' column since it's not useful in this dataset
economy_cleaned = economy.drop(columns=['Code'])

# Now the dataset is cleaned of the 'Code' column, you can proceed with further transformations if needed
# For example, checking for missing values or converting the 'Total economic damages' column to numeric
economy_cleaned['Total economic damages'] = pd.to_numeric(economy_cleaned['Total economic damages'], errors='coerce')

# Check for remaining missing values
missing_values = economy_cleaned.isnull().sum()

# Save the cleaned dataset (optional)
economy_cleaned.to_csv('cleaned_economic_damage.csv', index=False)

print(economy_cleaned.head())  # Display the cleaned dataset

#EMDAT TABLE --CLEANING
events= pd.read_excel('public_emdat_custom_request_2024-06-26_65811dbf-f6a0-43b0-a0af-a151ab0b3ee7.xlsx')
print(events.columns)

events_cleaned= events.drop(columns=['DisNo.', 'Historic', 'External IDs', 'Origin', 'Associated Types', 'River Basin','CPI', 'Admin Units', 'Entry Date', 'Last Update'])
print(events_cleaned.head())


events_cleaned['Event Name'].fillna(MISSING_STRING, inplace=True)
events_cleaned['Location'].fillna(MISSING_STRING_2, inplace=True)
events_cleaned['Location']=events_cleaned['Location'].str.split(',')
events_cleaned = events_cleaned.explode('Location')
events_cleaned['Location'].str.strip(' ')
missing_condition = events_cleaned['Magnitude'].isnull() | events_cleaned['Magnitude Scale'].isnull()
events_cleaned.loc[missing_condition, ['Magnitude', 'Magnitude Scale']] = MISSING_STRING

country_coords={}
for country in events_cleaned['Country'].unique():
    lat,lon=get_lat_long(country)
    country_coords[country] = (lat,lon)

for idx, row in events_cleaned.iterrows():
    if pd.isnull(row['Latitude']) or pd.isnull(row['Longitude']):
        country_name = row['Country']
        lat, lon = country_coords.get(country_name, (None, None))
        events_cleaned.at[idx, 'Latitude'] = lat
        events_cleaned.at[idx, 'Longitude'] = lon

events_cleaned['AID Contribution (\'000 US$)'].fillna(0, inplace=True)
events_cleaned['Total Damage, Adjusted (\'000 US$)'].fillna(0, inplace=True)
events_cleaned['Total Damage (\'000 US$)'].fillna(0, inplace=True)
events_cleaned['Insured Damage (\'000 US$)'].fillna(0, inplace=True)
events_cleaned['Insured Damage, Adjusted (\'000 US$)'].fillna(0, inplace=True)
events_cleaned['Reconstruction Costs (\'000 US$)'].fillna(0, inplace=True)
events_cleaned['Reconstruction Costs, Adjusted (\'000 US$)'].fillna(0, inplace=True)

events_cleaned.to_csv('cleaned_emdat_dataset.csv', index=False)

missing_values_count = events_cleaned.isnull().sum()

# Print the columns with their corresponding missing values
print("Columns with Missing Values:")
for column, missing_count in missing_values_count.items():
    if missing_count>0:
        print(f"{column}: {missing_count} missing values")