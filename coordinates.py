import pandas as pd
from geopy.geocoders import Nominatim
import json
from time import sleep
from datetime import datetime
# Initialize the geolocator
geolocator = Nominatim(user_agent="geoapi")

# Function to get latitude and longitude for a given country
def get_coordinates(location):
    try:
        coordinates = geolocator.geocode(location, timeout=10)
        if coordinates:
            return coordinates.latitude, coordinates.longitude
        else:
            return None, None
    except Exception as e:
        print(f"Error fetching location for {location}: {e}")
        return None, None

def generate_coords(table):
    print(f"Unique values in 'Location': {table['Location'].nunique()}")
    print(f"Unique values in 'Country': {table['Country'].nunique()}")
    print(f"Unique values in 'ISO': {table['ISO'].nunique()}")
    location_coords = {}
    index_row=0
    for i,row in table.iterrows():
        location = row['Location']
        country = row['Country']
        iso_code = row['ISO']
        lat = None
        lon = None
        #print(location, country, iso_code)
        if pd.notnull(location):
            lat, lon = get_coordinates(location)
            #print(f"Attempt for Location '{location}': {lat}, {lon}")
        if (lat is None) and (lon is None) and pd.notnull(country):
            #print(f"Location '{location}' not found. Retrying with Country: {country}")
            lat, lon = get_coordinates(country)
            #print(f"Attempt for Country '{country}': {lat}, {lon}")
        if (lat is None) and (lon is None) and pd.notnull(iso_code):
            #print(f"Country '{country}' not found. Retrying with ISO: {iso_code}")
            lat, lon = get_coordinates(iso_code)
            #print(f"Attempt for ISO '{iso_code}': {lat}, {lon}")
        
        if (lat is not None) and (lon is not None):
            lat = str(lat).replace('.', ',')
            lon = str(lon).replace('.', ',')
        if index_row % 450 == 0:
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"{index_row/45000 *100}% -- {current_time}")
        location_coords[index_row] = (lat, lon)
        index_row+=1
        #sleep(1)
    
    return location_coords

def save_country_coords_to_file(country_coords, file_path):
    with open(file_path, 'w') as file:
        json.dump(country_coords, file)

def load_country_coords_from_file(file_path):
    with open(file_path, 'r') as file:
        country_coords = json.load(file)
    return country_coords


def fill_lat_long_from_dict(table, country_coords):
    for idx, row in table.iterrows():
        if pd.isnull(row['Latitude']) or pd.isnull(row['Longitude']):
            country_name = row['Country']
            lat, lon = country_coords.get(country_name, (None, None))
            table.at[idx, 'Latitude'] = lat
            table.at[idx, 'Longitude'] = lon
    return table

if __name__ == "__main__":
    events= pd.read_excel('datasets/public_emdat_custom_request_2024-06-26_65811dbf-f6a0-43b0-a0af-a151ab0b3ee7.xlsx')
    #events['Location'].fillna('UNKNOWN', inplace=True)
    events['Location']=events['Location'].str.split(',')
    events = events.explode('Location')
    events['Location'].str.lstrip(' ')
    country_coords = generate_coords(events)
    save_country_coords_to_file(country_coords, 'utils/country_coords.json')