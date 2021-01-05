# -*- coding: utf-8 -*-
"""
Created on Sun Dec 20 20:14:06 2020

@author: oksan
"""

import mysql.connector
import  pandas as pd
import json
import numpy as np
from datetime import datetime
from datetime import timedelta
import sys

#connection params
host = 'localhost'
user = 'oleg_user'
passwd = 'kz59qj07w'
database = 'appmetrica'
table = 'attribution'

#get attribution data
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()
mycursor.execute('SELECT * FROM attribution')
attributions_list = mycursor.fetchall()
db_connection.close()

attributions_df = pd.DataFrame(attributions_list)
attributions_df.columns = ['appmetrica_device_id','tracker_name','os_name','install_datetime']
attributions_df.appmetrica_device_id = pd.to_numeric(attributions_df.appmetrica_device_id)
attributions_df = attributions_df.sort_values('install_datetime', inplace = False)
attributions_df = attributions_df.drop_duplicates(subset ='appmetrica_device_id',keep = 'last', inplace = False)

#get events data
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()

yesterday = datetime.today() - timedelta(days=1)
yesterday_ymd = yesterday.strftime('%Y-%m-%d')

mycursor.execute('SELECT * FROM events WHERE event_name = "order_paid" AND event_datetime = ("' + yesterday_ymd + '") ORDER BY event_datetime')
purchases = mycursor.fetchall()
db_connection.close()

if len(purchases) == 0:
    sys.exit()

#convert list to df
df_purchases0 = pd.DataFrame(purchases)
df_purchases0.columns = ['appmetrica_device_id','event_name','event_json','os_name','event_datetime']
df_purchases0 = df_purchases0.reset_index(drop=True)
ser_purchases = df_purchases0.event_json
ser_purchases = ser_purchases.reset_index(drop=True)

def dict_df_iter(number: str):
    dict_purchase = json.loads(ser_purchases[number])
    dict_purchase.update({'appmetrica_device_id': df_purchases0.appmetrica_device_id[number]})
    dict_purchase.update({'os_name': df_purchases0.os_name[number]})
    dict_purchase.update({'event_datetime': df_purchases0.event_datetime[number]})
    df_purchases_iter = pd.DataFrame([dict_purchase], columns=dict_purchase.keys())
    #del df_purchases_iter['product_ids']
    del df_purchases_iter['order_products']
    df_purchases_iter1 = pd.DataFrame.from_dict(dict_purchase['order_products'])
    df_purchases_iter1 = df_purchases_iter1.T
    df_purchases_iter1 = df_purchases_iter1.reset_index(drop=True)
    if len(df_purchases_iter1) > 1:
        df_purchases_iter = df_purchases_iter.append([df_purchases_iter.iloc[0]]*(len(df_purchases_iter1)-1),ignore_index=True)   
    df_purchases_iter2 = pd.concat([df_purchases_iter,df_purchases_iter1], axis = 1)
    return df_purchases_iter2

df_purchases_mod = pd.DataFrame(columns=dict_df_iter(number = 0).columns)
for index,value in ser_purchases.items():
    iter_df = dict_df_iter(number = index)
    df_purchases_mod = pd.concat([df_purchases_mod,iter_df],ignore_index=True)
   
#install attribution add
df_purchases_mod.appmetrica_device_id = pd.to_numeric(df_purchases_mod.appmetrica_device_id)
df_purchases_ready = df_purchases_mod.merge(attributions_df[['appmetrica_device_id','tracker_name']], 'left')
df_purchases_ready = df_purchases_ready.fillna(0)
purchases_list = df_purchases_ready.values.tolist()

#transorm inner lists to str
for i in range(0, len(purchases_list)):
    list_iter = purchases_list[i]
    for j in range(0, len(list_iter)):
        if type(list_iter[j]) == list:
            list_iter[j] = str(list_iter[j])

#write to mysql table
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()

#create_table
#col_names = list(df_purchases_ready.columns.values + ' VARCHAR(255)')
#col_names = ','.join(col_names)
#mycursor.execute('CREATE TABLE purchases (' + col_names + ')')
#insert
col_names = list(df_purchases_ready.columns.values)
col_names = ','.join(col_names)
col_names_index = np.repeat('%s', 16, axis=0)
col_names_index = ','.join(col_names_index)
sql_query = 'INSERT INTO purchases (' + col_names + ') VALUES (' + col_names_index + ')'
mycursor.executemany(sql_query,purchases_list)
db_connection.commit()
db_connection.close()
