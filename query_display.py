import psycopg2
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler

current_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(current_dir, 'db_tables')


#Function to execute the query which reads the queries from the files and returns the results in a list
def execute_query(connection, num_queries):
    list_results=[]
    with connection.cursor() as cursor:
        for i in range(1,num_queries+1):
            with open(f'db_tables/query_{i}.sql', "r", encoding="utf-8") as file:
                query = file.read()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            #The results are saved in the pandas dataframe
            result_table=pd.DataFrame(results, columns=columns)
            list_results.append(result_table)
    return list_results

if __name__=="__main__":
    connection=None
    try:
        connection= psycopg2.connect(dbname="Dm-Project", user='postgres', password='1234', host='localhost',port='5432')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    #number of queries to compute
    num_queries=7
    result_list=execute_query(connection, num_queries)

    #Printing the results of all the queries
    for i in result_list:
        print('query\n',i)


    #Plot query 1
    query_name='Frequency of disaster subgroups by continent'
    plt.figure(figsize=(12, 6))
    #plotting a barplot with continent on the x axis
    sns.barplot(data=result_list[0], x='continent', y='disaster_count', hue='disaster_subgroup')
    plt.title(query_name)
    plt.xlabel('Continent')
    plt.ylabel('Disaster Count')
    plt.xticks(rotation=45)
    plt.legend(title='Disaster subgroup')
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()

    #Plot query 2
    query_name_deaths = 'total deaths by disaster group for each decade'
    query_name_injuries = 'total injuries by disaster group for each decade'
    #Here I created two images one for the total deaths and another for the total injuries
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

    #Plot query 3
    query_name= 'Top 10 world regions that received the most aid in the last 20 years'
    plt.figure(figsize=(9, 8))
    plt.table(cellText=result_list[2].values, colLabels=result_list[2].columns, cellLoc='center', loc='center', colColours=['#f5f5f5']*len(result_list[2].columns))
    plt.axis('off')  # Hide the axes
    plt.title(query_name)
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()

    query_name = 'Top 10 world regions that received the most aid in the last 20 years'
    query_3= result_list[2].sort_values(by="total_aid_contribution", ascending=False)

    plt.figure(figsize=(12, 6))
    ax=sns.barplot(data=query_3, x='area', y='total_aid_contribution', hue='area', errorbar=None, palette="Blues_r")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{int(x):,}'))
    plt.title(query_name)
    plt.xlabel("Region", fontsize=12)
    plt.xticks(rotation=25)
    plt.tight_layout()
    plt.ylabel("Total Aid Contribution", fontsize=12)
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()

    #Plot query 4
    query_name = 'Correlation Between Average Emissions and Event Total Damage for Each Year'
    query_4 = result_list[3]
    #In order to help the visualization I normalized the avg_emission and total damage columns values by using
    #minmaxscaler
    scaler = MinMaxScaler()
    query_4[['avg_emissions', 'total_damage']] = scaler.fit_transform(query_4[['avg_emissions', 'total_damage']])
    
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=query_4,x='years', y='avg_emissions',marker='o' , color='lightblue', label='Avg Emissions')
    sns.lineplot(data=query_4, x='years', y='total_damage', marker='o', color='red', label='Total Damage')
    plt.title(query_name)
    plt.xlabel('Year')
    plt.ylabel('Normalized values')
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    #plt.show()
    plt.close()

    #Plot query 5
    query_name="Most affected nations by disaster type"
    query_5 =result_list[4]

    plt.figure(figsize=(12, 6))
    sns.barplot(data=query_5, x='total_affected', y='nation', hue='disaster_type', dodge=False, palette="viridis")
    plt.title(query_name)
    plt.xlabel("Total Affected Individuals", fontsize=12)
    plt.ylabel("Nation", fontsize=12)
    plt.legend(title="Disaster Type", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()

    #Plot query_6
    query_name="Frequency of disaster according to months"
    query_6=result_list[5]
    
    plt.figure(figsize=(14, 8))
    sns.barplot(data=query_6, x='month_string', y='disaster_count', hue='disaster_type', palette="tab10")
    plt.title(query_name)
    plt.xlabel("Month", fontsize=12)
    plt.ylabel("Disaster Count", fontsize=12)
    plt.legend(title="Disaster Type", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()

    #Plot query 7
    query_name = "Comparison of average deaths by disaster subgroup in 1960s vs 2010s"
    query_7 = result_list[6]

    plt.figure(figsize=(12, 8))
    sns.barplot(data=query_7, y='disaster_subgroup', x='avg_deaths', hue='decade', palette='coolwarm')
    plt.title(query_name)
    plt.xlabel('Average Deaths')
    plt.ylabel('Disaster Subtype')
    plt.legend(title='Decade', loc='upper right')
    plt.tight_layout()
    plt.savefig(f'imgs/{query_name.lower().replace(" ", "_")}.png')
    plt.close()


