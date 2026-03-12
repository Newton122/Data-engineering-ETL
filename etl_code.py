import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3


url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'


# Code for ETL operations on Country-GDP data

# Importing the required libraries


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    df['GDP_USD_millions'] = df['GDP_USD_millions'].replace(r'[\$,]', '', regex=True).astype(float)
    df["GDP_USD_millions"] = np.round(df["GDP_USD_millions"]/1000, 2)
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
   ''' This function saves the final dataframe as a `CSV` file 
   in the provided path. Function returns nothing.'''
   df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
   ''' This function saves the final dataframe as a database table
   with the provided name. Function returns nothing.'''
   df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql_query(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = dt.datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("etl_log.txt","a") as log_file:
        log_file.write(f"{timestamp} : {message}\n")

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''



log_progress("ETL Job Started")

df = extract(url, table_attribs)

log_progress("Data Extraction Complete.Initializing Transformation Process")

df = transform(df)

log_progress("Data Transformation Complete.Initializing Load Process")

load_to_csv(df, csv_path)

sql_connection = sqlite3.connect(db_name)

log_progress("Loading data to database")

load_to_db(df, sql_connection, table_name)

log_progress("Data Load Complete. Running Queries")

query_statement = "SELECT Country, GDP_USD_billions FROM [Countries_by_GDP] WHERE GDP_USD_billions > 1000 ORDER BY GDP_USD_billions DESC"
run_query(query_statement, sql_connection)

log_progress("ETL Job Completed")

sql_connection.close()
