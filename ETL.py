import pandas as pd
from openpyxl import load_workbook, Workbook
import json


#CONSTANTS
#I used three constants for dealing with missing values, the first value is more general, while the 
#second one is used in the case of the place of a disaster since it is known but it is not specified
#in the database
MISSING_STRING='unknown'
MISSING_STRING_2='Not specified'
MISSING_NUMBER=0

#These dictionaries map the name of each month to a number and viceversa
MONTH_INT={'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12,
    'DecJanFeb':13, 'MarAprMay':14, 'JunJulAug':15,
    'SepOctNov':16, 'Meteorological year':17
}

INT_MONTH = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}


def normalize_table(table):
    #This function normalizes the elements, if there is a missing value and the column is a string
    #It fills it with the Constant, lowers the characters and removes spaces
    #If the columns is an number or a boolean it fills them with the respective empty values
    for column in table.columns:
        if table[column].isnull().any():
            if table[column].dtype == 'object':
                table[column] = table[column].fillna(MISSING_STRING)
                table[column] = table[column].str.lower().str.strip()

            elif pd.api.types.is_numeric_dtype(table[column]):
                table[column]=table[column].fillna(MISSING_NUMBER)

            elif table[column].dtype == 'bool':
                table[column]=table[column].fillna(False)
    return table

def load_location_coords(path):
    with open(path, 'r') as file:
        country_coords = json.load(file)
    return country_coords

def clean_event(event):
    #First I deleted all the non relevant columns for example the fields related to admins and to the update of the old table
    event_cleaned = event.drop(columns=['Historic', 'External IDs', 'Origin', 'Associated Types', 'River Basin','CPI', 
                                         'Admin Units', 'Entry Date', 'Last Update', 'DisNo.', 'Classification Key'])
    
    #Since all the columns were in the string format I ensured that I converted all the appropriate columns into numeric
    numeric_columns=['Latitude', 'Longitude', 'Magnitude', 'Start Year', 'Start Month','Start Day', 'End Year', 'End Month', 'End Day', 'Total Deaths'
                     ,'No. Injured', 'No. Affected', 'No. Homeless', 'Total Affected', 'AID Contribution (\'000 US$)', 'Total Damage, Adjusted (\'000 US$)'
                     ,'Total Damage (\'000 US$)', 'Insured Damage (\'000 US$)','Insured Damage, Adjusted (\'000 US$)', 'Reconstruction Costs (\'000 US$)'
                     , 'Reconstruction Costs, Adjusted (\'000 US$)']
    for col in numeric_columns:
        event_cleaned[col] = pd.to_numeric(event_cleaned[col], errors='coerce')

    #I fill the null values witht the missing string 'Not specified'
    event_cleaned['Location']=event_cleaned['Location'].fillna(MISSING_STRING_2)
    #Since locations can have more than one place separated by a coma I created an array by using split
    event_cleaned['Location']=event_cleaned['Location'].str.split(',')
    #Explode creates a new row for each one of the value in each location array
    event_cleaned = event_cleaned.explode('Location')
    #I remove the right and left spaces from the locations, that are left since the split
    event_cleaned['Location']=event_cleaned['Location'].str.strip()
    event_cleaned = event_cleaned[event_cleaned['Location'] != '']

    #I noticed that the coordinates have a lot of empty values, in order to solve that I used the json file containing 
    #the appropriate coordinates for each location that I created in the coordinates.py file
    location_coords=load_location_coords('utils/country_coords.json')

    for idx, row in event_cleaned.iterrows():
        if pd.isnull(row['Latitude']) or pd.isnull(row['Longitude']):
            lat, lon = location_coords.get(str(idx))
            lat = lat.replace(',', '.')
            lon = lon.replace(',', '.')
            event_cleaned.at[idx, 'Latitude'] = float(lat)
            event_cleaned.at[idx, 'Longitude'] = float(lon)
            #I do a loop in the elements and check whether the element in the table is null
            #then I extract the result and insert it into the row after converting it into a float value

    event_cleaned['Magnitude']= event_cleaned['Magnitude'].astype(float)


    event_cleaned['ISO'] = event_cleaned['ISO'].str.lower()
    
    date_columns = ['Start Year','Start Month', 'Start Day','End Year', 'End Month', 'End Day']

    # Print missing values for the specified columns
    for column in date_columns:
        if column in ['Start Year','Start Month', 'Start Day']:
            event_cleaned[column] = event_cleaned[column].fillna(MISSING_NUMBER+1)
        event_cleaned[column] = event_cleaned[column].astype('Int64')
   
    event_cleaned['End Month']=event_cleaned['End Month'].fillna(event_cleaned['Start Month'])
    event_cleaned['End Day']=event_cleaned['End Day'].fillna(event_cleaned['Start Day'])
        
    event_cleaned['start_date'] = pd.to_datetime(
        event_cleaned[['Start Year', 'Start Month', 'Start Day']].astype(str).agg('-'.join, axis=1),
        format='%Y-%m-%d',
        errors='coerce'
    )
    
    event_cleaned['end_date'] = pd.to_datetime(
        event_cleaned[['End Year', 'End Month', 'End Day']].astype(str).agg('-'.join, axis=1),
        format='%Y-%m-%d',
        errors='coerce'
    )
  

    event_cleaned['duration'] = (event_cleaned['end_date'] - event_cleaned['start_date']).dt.days

    

    event_cleaned['duration'] = event_cleaned['duration'].replace(0, 1)

    event_cleaned = event_cleaned.drop(columns=['start_date', 'End Year', 'End Month', 'End Day'])
    event_cleaned= event_cleaned.rename(columns={'Region':'continent', 'Subregion':'area'})
    #For better clarity I inserted the Start month and the end month also in the string format
    event_cleaned['continent_id'] = pd.factorize(event_cleaned['continent'])[0]
    event_cleaned['area_id'] = pd.factorize(event_cleaned['area'])[0]

    event_cleaned['global_name']= 'World'

    event_cleaned.rename(columns={'Start Month': 'Start Month Number'}, inplace=True)
    event_cleaned['Start Month'] = event_cleaned['Start Month Number'].map(INT_MONTH)

    #I apply the normalize_table function
    event_cleaned=normalize_table(event_cleaned)

    return event_cleaned

def clean_economy(economy):
    #The cleaning of the economy table is very simple I drop the code columns
    economy_cleaned = economy.drop(columns=['Code'])
    #I rename all the entity disaster values to match the ones in the event table
    economy_cleaned['Entity'] = economy_cleaned['Entity'].replace({'Dry mass movement': 'Mass movement (dry)', 'Wet mass movement': 'Mass movement (wet)', 'Extreme weather': 'Storm'})
    economy_cleaned=normalize_table(economy_cleaned)
    return economy_cleaned

def clean_temperature(temperature):
    #I wanted to create a column containing all the values of the year, instead of many columns each one with its
    #own value, so I use the function melt to create two new columns 'Year' and 'Value' which contains the label of
    #the year column and the value of each row for each year column respectively
    temperature_shift = pd.melt(temperature, 
                    id_vars=['Area Code', 'Area', 'Months Code', 'Months', 'Element', 'Unit','Element Code'], 
                    var_name='Year', 
                    value_name='Value')

    temperature_shift['Year'] = temperature_shift['Year'].str.replace('Y', '').astype(int)
    #I convert the values in the column Year to actual years and integers

    #based on the values of the column Element I create two columns based on the values
    cleaned_temperature = temperature_shift.pivot_table(index=['Area Code', 'Area', 'Months Code', 'Months', 'Year'], columns='Element', values='Value').reset_index()
    
    cleaned_temperature.columns.name = None
    cleaned_temperature.rename(columns={'Temperature change': 'Temperature Change', 
                           'Standard deviation': 'Standard Deviation'}, inplace=True)
    #I rename these two new columns according to the value
    
    cleaned_temperature=normalize_table(cleaned_temperature)
    #I drop the useless columns
    cleaned_temperature=cleaned_temperature.drop(columns=['Area Code', 'Months Code'])
    #I add my own version of the month number
    cleaned_temperature['Month Number'] = cleaned_temperature['Months'].map(MONTH_INT)
   
    cleaned_temperature = cleaned_temperature.dropna(subset=['Month Number'])
    #I drop the eventual null rows that contained trimester/semester
    cleaned_temperature['Month Number'] =cleaned_temperature['Month Number'].astype('Int64')
    numeric_cols= ["Temperature Change",  "Standard Deviation"]
    for c in numeric_cols:
        cleaned_temperature[c] = pd.to_numeric(cleaned_temperature[c], errors='coerce')
    cleaned_temperature.columns = cleaned_temperature.columns.str.lower().str.replace(' ', '_')
    cleaned_temperature.rename(columns={'area':'nation', 'months':'month_string', 'month_number':'month_int', 'year':'ev_year'}, inplace=True)


    return cleaned_temperature

def clean_emission(emission):
    num_col=['Coal','Oil','Gas','Cement','Flaring', 'Other', 'Per Capita']

    for c in num_col:
        emission[c] = pd.to_numeric(emission[c], errors='coerce')
        emission[c]= emission[c].astype(float)
    #The emission table doesn't need much adjustments
    cleaned_emission=normalize_table(emission)
    cleaned_emission.columns = cleaned_emission.columns.str.lower().str.replace(' ', '_')
    cleaned_emission.rename(columns={'country':'nation', 'iso_3166-1_alpha-3':'nation_iso',
                                             'coal':'coal_emission','oil':'oil_emission','gas':'gas_emission','cement':'cement_emission','flaring':'flaring_emission',
                                             'other':'other_emission', 'per_capita':'per_capita_emission', 'year':'ev_year'}, inplace=True)
    return cleaned_emission

def complete_event(event, economy):
    #In this function I want to use the values in the column 'total economic damages' in the economy table
    #to fill the empty values in the Total damage column in the event table

    #To do this I had to do the join between the two tables so I renamed the economy columns
    economy.rename(columns={'Entity': 'Disaster Type', 'Year': 'Start Year'}, inplace=True)
    merged_table = event.merge(economy, on=['Start Year', 'Disaster Type'], how='left')
    #I filled the empty values with zero
    merged_table['Total economic damages'] = merged_table['Total economic damages'].fillna(0)
    
    #I create a column 'Size' which contains the count for each year and disaster type of the rows that 
    #have value zero
    merged_table['Size']=merged_table.groupby(['Start Year', 'Disaster Type'])["Total Damage (\'000 US$)"].transform(lambda x: (x==0).sum())
 
    #I replace the zero with ones in the size column
    merged_table['Size']= merged_table['Size'].replace(0, 1)

    #I divide the cost in the total economic damages column for the number in 'size', so that I obtained an 'average' cost 
    #for the events that did'nt have a specified cost
    merged_table["Total economic damages"] = merged_table["Total economic damages"] / merged_table['Size']
    #I fill the values of the Total Damage column with the medium values only if it is equal to zero
    merged_table.loc[merged_table["Total Damage (\'000 US$)"] != 0, 'Total economic damages'] = merged_table["Total Damage (\'000 US$)"]
  
    #I drop the useless columns
    merged_table= merged_table.drop(columns=['Size', "Total Damage (\'000 US$)"])

    merged_table.columns = merged_table.columns.str.lower().str.replace(' ', '_')
    merged_table.rename(columns={'iso': 'nation_iso', 'country': 'nation', 'location':'location_name', 
                                             'ofda/bha_response':'response', "aid_contribution_(\'000_us$)": "aid_contribution",
                                              'no._injured':'num_injured', 'no._affected':'num_affected', 'no._homeless':'num_homeless',
                                               "reconstruction_costs,_adjusted_(\'000_us$)":"reconstruction_cost_adjusted",
                                               "reconstruction_costs_(\'000_us$)":"reconstruction_cost",
                                               "insured_damage_(\'000_us$)":"insured_damage",
                                               "insured_damage,_adjusted_(\'000_us$)":"insured_damage_adjusted", 'total_economic_damages': 'total_damage',
                                               "total_damage,_adjusted_(\'000_us$)": "total_damage_adjusted",
                                                "start_year":"ev_year", "start_month":"month_string", "start_month_number":"month_int", "start_day":"ev_day"}, inplace=True)
    return merged_table

if __name__=='__main__':
    economy_table= pd.read_csv('datasets/economic-damage-from-natural-disasters.csv', encoding='latin1')
    event_table= pd.read_excel('datasets/public_emdat_custom_request_2024-06-26_65811dbf-f6a0-43b0-a0af-a151ab0b3ee7.xlsx')
    temperature_table = pd.read_csv('datasets/Environment_Temperature_change_E_All_Data_NOFLAG.csv', encoding='latin1')
    emission_table_1= pd.read_csv('datasets/GCB2022v27_MtCO2_flat.csv', encoding='latin1')

    #CLEANING economy table
    print('starting to clean economy table')
    economy_cleaned= clean_economy(economy_table)
    economy_cleaned.to_csv('cleaned_datasets/cleaned_economic_damage.csv', sep=';', decimal=',', index=False)
   

    #CLEANING event table
    print('starting to clean event table')
    event_cleaned=clean_event(event_table)
    event_cleaned.to_csv('cleaned_datasets/cleaned_event.csv', sep=';', decimal=',', index=False)
    
    
    #CLEANING temperature table
    print ('starting to clean temperature table')
    temperature_table_cleaned=clean_temperature(temperature_table)
    temperature_table_cleaned.to_csv('cleaned_datasets/cleaned_temperature.csv', sep=';', decimal=',', index=False)
    
    #CLEANING emission table
    print('starting to clean the emission tables')
    emission_table_1_cleaned=clean_emission(emission_table_1)
    emission_table_1_cleaned.to_csv('cleaned_datasets/cleaned_emission_1.csv', sep=';', decimal=',', index=False)

    #Joining event and economy table
    event_economy_integrated= complete_event(event_cleaned, economy_cleaned)
    event_economy_integrated.to_csv('cleaned_datasets/cleaned_event_econ.csv', sep=';', decimal=',', index=False)
    
    
