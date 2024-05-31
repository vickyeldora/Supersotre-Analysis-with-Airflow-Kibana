'''
=================================================
Name  : Vicky Eldora Wuisan

This program is designed to automate the transformation and loading of data from PostgreSQL to Elasticsearch. The dataset used pertains to superstore marketing campaign sales.
=================================================
'''

# Import Libraries

# PostgreSQL connection
import psycopg2 as db
# load data
import pandas as pd
import numpy as np

from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta
import warnings

# fetch from postgresql
def query_postgresql():
    '''
    This function is created to fetch data from a table in the PostgreSQL database and save it in CSV format.
    '''
    # connect to PostgreSQL with psycopg
    conn_string = "dbname='airflow' user='airflow' password='airflow' host='postgres' port='5432'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)
    df.to_csv('P2M3_vicky_eldora_data_raw.csv')
    print("-------Data Saved------")

# Since the data is still unorganized and unclean, I will perform data cleaning

def clean_data():
    '''
    The main purpose of creating this function is to clean the raw data retrieved from PostgreSQL.
    Filling missing values, Removing duplicate rows, Adjusting column names to be more consistent and follow convention,
    Handling outliers based on data distribution
    '''
    df = pd.read_csv('P2M3_vicky_eldora_data_raw.csv')

    # Calculate percentage of missing values
    missing_percent = df.isnull().mean() * 100
    
    # Fill missing values based on their percentage
    for col in df.columns:
        if missing_percent[col] > 5:
            if df[col].dtype == 'object':  # Categorical data
                df[col].fillna(df[col].mode()[0], inplace=True)
            else:  # Numerical data
                skewness = df[col].dropna().skew()
                if abs(skewness) < 0.5:  # Normal distribution
                    df[col].fillna(df[col].mean(), inplace=True)
                else:  # Skewed distribution
                    df[col].fillna(df[col].median(), inplace=True)
        else:
            df.dropna(subset=[col], inplace=True)
    
    # Remove duplicate rows
    df.drop_duplicates(inplace=True) 
    
    # Replace uppercase letters in the middle of words with underscores
    df.columns = df.columns.str.replace(r'(?<!^)(?=[A-Z])', '_', regex=True).str.lower()

    # Replace double underscores with a single underscore
    df.columns = df.columns.str.replace('__', '_')
    
    # Replace spaces in column names with underscores
    df.columns = df.columns.str.replace(' ', '_')
    
    # Remove unnecessary spaces/tabs/symbols in column names
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
    
    # Replace "mnt" with "amount" and "num" with "number" in column names
    df.columns = df.columns.str.replace('mnt', 'amount')
    df.columns = df.columns.str.replace('num', 'number')
    
    # Replace uppercase letters in the middle of words with underscores
    df.columns = df.columns.str.replace(r'(?<!^)(?=[A-Z])', '_', regex=True).str.lower()

    # Determine distribution of numerical data
    normal = []
    skew = []
    extreme_skew = []

    for col in df.select_dtypes(include=[np.number]).columns:
        skewness = df[col].skew()
        if -0.5 < skewness < 0.5:
            normal.append(col)
        elif -1 <= skewness <= -0.5 or 0.5 <= skewness <= 1:
            skew.append(col)
        elif skewness < -1 or skewness > 1:
            extreme_skew.append(col)

    # Handling outliers
    for col in df.select_dtypes(include=[np.number]).columns:
        if df[col].nunique() <= 2:  # Skip outlier handling for binary columns
            continue
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
        outlier_percent = (outliers.shape[0] / df.shape[0]) * 100

        if outlier_percent > 5:
            if col in normal:
                lower_cap = df[col].mean() - 3 * df[col].std()
                upper_cap = df[col].mean() + 3 * df[col].std()
            elif col in skew or col in extreme_skew:
                lower_cap = q1 - 1.5 * iqr
                upper_cap = q3 + 1.5 * iqr
            df[col] = np.clip(df[col], lower_cap, upper_cap)
        else:
            df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
            
    # Reset index
    df.reset_index(drop=True, inplace=True)

    # Save the cleaned data.
    df.to_csv('./dags/P2M3_vicky_eldora_data_clean.csv')

# insert into elastic search - with airflow
def insert_elasticsearch():
    '''
    This function inserts the cleaned data into Elasticsearch. Each row of data is converted into a JSON format and inserted into the "superstore" index.
    '''
    es = Elasticsearch('http://elasticsearch:9200')
    df = pd.read_csv('./dags/P2M3_vicky_eldora_data_clean.csv')
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="superstore", doc_type="_doc", body=doc)
        print(res)


default_args = {
    'owner': 'vicky',
    'start_date': datetime(2024, 5, 23),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
with DAG(
    'p2m3',
    default_args=default_args,
    schedule_interval='30 6 * * *',  # every 6.30 in the morning
    catchup=False
) as dag:

    # Initial task to display a message in the terminal.
    node_start = BashOperator(
        task_id='starting',
        bash_command='echo "I am reading the CSV now..."'
    )
    # Fetch data from PostgreSQL.
    getData = PythonOperator(
        task_id='QueryPostgreSQL',
        python_callable=query_postgresql
    )
    # Clean the fetched data
    cleanData = PythonOperator(
        task_id='CleaningData',
        python_callable=clean_data
    )
     # Insert the cleaned data into Elasticsearch.
    insertData = PythonOperator(
        task_id='InsertDataElasticSearch',
        python_callable=insert_elasticsearch
    )

# Define task dependencies
node_start >> getData >> cleanData >> insertData