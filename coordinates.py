import pandas as pd
from geopy.geocoders import Nominatim
import json
from time import sleep

# Initialize the geolocator
geolocator = Nominatim(user_agent="geoapi")

# Function to get latitude and longitude for a given country
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

# Step 1: Create and populate the country_coords dictionary
def create_country_coords(df):
    country_coords = {}
    for country in df['Country'].unique():
        lat, lon = get_lat_long(country)
        country_coords[country] = (lat, lon)
        # Sleep to avoid overwhelming the geolocation service
        sleep(1)
    return country_coords

# Step 2: Save the dictionary to a file (JSON format)
def save_country_coords_to_file(country_coords, file_path):
    with open(file_path, 'w') as file:
        json.dump(country_coords, file)

# Step 3: Load the dictionary from a file
def load_country_coords_from_file(file_path):
    with open(file_path, 'r') as file:
        country_coords = json.load(file)
    return country_coords

# Step 4: Use the loaded dictionary to fill in the Latitude and Longitude columns
def fill_lat_long_from_dict(df, country_coords):
    for idx, row in df.iterrows():
        if pd.isnull(row['Latitude']) or pd.isnull(row['Longitude']):
            country_name = row['Country']
            lat, lon = country_coords.get(country_name, (None, None))
            df.at[idx, 'Latitude'] = lat
            df.at[idx, 'Longitude'] = lon
    return df

# Example DataFrame (replace with your EMDAT dataset)
data = {'Country': ['United States', 'Japan', 'Jamaica', 'Brazil', 'Japan'],
        'ISO': ['USA', 'JPN', 'JAM', 'BRA', 'JPN'],
        'Latitude': [None, None, None, None, None],
        'Longitude': [None, None, None, None, None]}
df = pd.DataFrame(data)

# Create the country_coords dictionary (this only needs to be done once)
country_coords = create_country_coords(df)

# Save the dictionary to a JSON file
save_country_coords_to_file(country_coords, 'country_coords.json')