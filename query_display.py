import psycopg2
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler

current_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(current_dir, 'db_tables')

def execute_query(connection, num_queries):
    list_results=[]
    with connection.cursor() as cursor:
        for i in range(1,num_queries+1):
            with open(f'db_tables/query_{i}.sql', "r", encoding="utf-8") as file:
                query = file.read()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            result_table=pd.DataFrame(results, columns=columns)
            list_results.append(result_table)
    return list_results

if __name__=="__main__":
    connection=None
    try:
        connection= psycopg2.connect(dbname="Dm-Project", user='postgres', password='1234', host='localhost',port='5432')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    with open('db_tables/query_1.sql', "r", encoding="utf-8") as file:
        query = file.read()
    num_queries=6
    result_list=execute_query(connection, num_queries)

    for i in result_list:
        print('query\n',i)


    #Print query 1
    query_name='Frequency of disaster subgroups by continent'
    plt.figure(figsize=(12, 6))
    sns.barplot(data=result_list[0], x='continent', y='disaster_count', hue='disaster_subgroup')
    plt.title(query_name)
    plt.xlabel('Continent')
    plt.ylabel('Disaster Count')
    plt.xticks(rotation=45)
    plt.legend(title='Disaster subgroup')
    #plt.show()
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()

    #Print query 2
    query_name_deaths = 'total deaths by disaster group for each decade'
    query_name_injuries = 'total injuries by disaster group for each decade'

    plt.figure(figsize=(14, 8))
    sns.barplot(data=result_list[1], x='decade', y='total_deaths', hue='disaster_group', errorbar=None, palette='Reds')
    plt.title(query_name_deaths)
    plt.xlabel('Decade')
    plt.ylabel('Total deaths')
    plt.legend(title='Disaster group')
    plt.savefig(f'imgs/{query_name_deaths.lower().replace(" ", "_")}.png')
    plt.close()

    plt.figure(figsize=(14, 8))
    sns.barplot(data=result_list[1], x='decade', y='total_injuries', hue='disaster_group', errorbar=None, palette='Blues')
    plt.title(query_name_injuries)
    plt.xlabel('Decade')
    plt.ylabel('Total injuries')
    plt.legend(title='Disaster group')
    plt.savefig(f'imgs/{query_name_injuries.lower().replace(" ", "_")}.png')
    plt.close()

    #Print query 3
    query_name= 'Top 10 world regions that received the most aid in the last 20 years'
    plt.figure(figsize=(9, 8))
   
    plt.table(cellText=result_list[2].values, colLabels=result_list[2].columns, cellLoc='center', loc='center', colColours=['#f5f5f5']*len(result_list[2].columns))
    plt.axis('off')  # Hide the axes
    plt.title(query_name)
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()

    import matplotlib.pyplot as plt

    query_name = 'Top 10 world regions that received the most aid in the last 20 years'
    plt.figure(figsize=(7, 6))

    # Create the table
    table = plt.table(cellText=result_list[2].values, colLabels=result_list[2].columns, cellLoc='center', loc='center', colColours=['#f5f5f5']*len(result_list[2].columns))

    # Set vertical padding by modifying row heights
    for i, key in enumerate(table.get_celld().keys()):
        cell = table.get_celld()[key]
        cell.set_fontsize(14)
        cell.set_text_props(color='black')
        cell.set_facecolor(color='white') 

        cell.set_height(0.08)  # Adjust the value to increase vertical padding

        # Set borders for better visibility
        cell.set_edgecolor('gray')
        if key[1] == 1 and i!=21:  
            #Check if the column is the second column and skip the title of the column
            original_value = float(cell.get_text().get_text()) 
            #Get the text object from the cell and then get the text
            formatted_text = f'${original_value:,.2f}'
            cell._text.set_text(formatted_text)

    plt.axis('off')
    plt.title(query_name)
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()

    #query 4
    query_name = 'Correlation Between Average Emissions and Event Total Damage for Each Year'
    query_4 = result_list[3]
    # Normalize the columns using Min-Max Scaling
    scaler = MinMaxScaler()
    query_4[['avg_emissions', 'total_damage']] = scaler.fit_transform(query_4[['avg_emissions', 'total_damage']])
    
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=query_4,x='years', y='avg_emissions',marker='o' , color='lightblue', label='Avg Emissions')
    sns.lineplot(data=query_4, x='years', y='total_damage', marker='o', color='red', label='Total Damage')
    plt.title(query_name)
    plt.xlabel('Year')
    plt.ylabel('Normalized values')

    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.show()
    plt.close()

