import pandas as pd
from geopy.geocoders import Nominatim
import json
from time import sleep
from datetime import datetime
#To get the coordinates I used geoapy, which given the name of a location, a country or a continent, it
#return the latitude and the longitude of its exact center

geolocator = Nominatim(user_agent="geoapi")

def get_coordinates(location):
    #This function given a certain location it gets the coordinates by making a request to the 
    #geoapi server
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
    #This function Iterates in each row of the table, and It tries to generate the coordinates 
    #for each row. This process is done by first looking at the value in location
    #If the coordinates are not found then the same process is repeated for the country,
    #and at the end in the ISO code, since ISO is present in each row (has no missing values)
    location_coords = {}
    index_row=0
    for i,row in table.iterrows():
        location = row['Location']
        country = row['Country']
        iso_code = row['ISO']
        lat = None
        lon = None
        print(location, country, iso_code)

        if pd.notnull(location):
            lat, lon = get_coordinates(location)
            print(f"Attempt for Location '{location}': {lat}, {lon}")

        if (lat is None) and (lon is None) and pd.notnull(country):
            print(f"Location '{location}' not found. Retrying with Country: {country}")
            lat, lon = get_coordinates(country)
            print(f"Attempt for Country '{country}': {lat}, {lon}")

        if (lat is None) and (lon is None) and pd.notnull(iso_code):
            print(f"Country '{country}' not found. Retrying with ISO: {iso_code}")
            lat, lon = get_coordinates(iso_code)
            print(f"Attempt for ISO '{iso_code}': {lat}, {lon}")
        
        if (lat is not None) and (lon is not None):
            lat = str(lat).replace('.', ',')
            lon = str(lon).replace('.', ',')

        if index_row % 450 == 0:
            #print the timestamp
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"{index_row/45000 *100}% -- {current_time}")

        location_coords[index_row] = (lat, lon)
        index_row+=1
        sleep(1)
        #The execution of this function takes a lot of time
    return location_coords

def save_country_coords_to_file(country_coords, file_path):
    with open(file_path, 'w') as file:
        json.dump(country_coords, file)

def load_country_coords_from_file(file_path):
    with open(file_path, 'r') as file:
        country_coords = json.load(file)
    return country_coords


if __name__ == "__main__":
    #I open the event dataset
    events= pd.read_excel('datasets/public_emdat_custom_request_2024-06-26_65811dbf-f6a0-43b0-a0af-a151ab0b3ee7.xlsx')
    #I split each location into an array
    events['Location']=events['Location'].str.split(',')
    #I create a row for each location in the array
    events = events.explode('Location')
    #I eliminate the spaces
    events['Location'].str.lstrip(' ')
    country_coords = generate_coords(events)
    #I save the coordinates into a json file
    save_country_coords_to_file(country_coords, 'utils/country_coords.json')