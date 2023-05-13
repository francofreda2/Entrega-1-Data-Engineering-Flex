# -*- coding: utf-8 -*-
"""
Created on Sat May 13 11:28:46 2023

@author: USER
"""

import psycopg2
from psycopg2.extras import execute_values
import requests
import pandas as pd

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base="data-engineer-database"
user="fredafranco13_coderhouse"
pwd= "uuS4769kWq"
conn = psycopg2.connect(
    host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    dbname=data_base,
    user=user,
    password=pwd,
    port='5439'
)

request_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin"

r = requests.get(request_url)

status_code=r.status_code

if (status_code==200):
    print("Request exitoso")
    response_data=r.json()
    response_df = pd.json_normalize(response_data)
    response_df=response_df[['id','symbol','current_price','ath','ath_date']]
    
    # Convert data types to native Python types
    response_df['current_price'] = response_df['current_price'].astype(float)
    response_df['ath'] = response_df['ath'].astype(float)

    # Insert the data into the Redshift table
    with conn.cursor() as cursor:
        insert_query = """
        INSERT INTO cryptocurrencies (id, symbol, current_price, ath, ath_date)
        VALUES (%s, %s, %s, %s, %s);
        """
        data_to_insert = tuple(response_df.iloc[0].values)
        cursor.execute(insert_query, data_to_insert)
        conn.commit()
        print("Data inserted successfully.")
else:
    print("Error!")

conn.close()
