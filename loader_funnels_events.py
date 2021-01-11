# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 22:09:45 2021

@author: oksan
"""

import mysql.connector
import  pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta

#connection params
host = 'localhost'
user = 'oleg_user'
passwd = 'kz59qj07w'
database = 'appmetrica'

yesterday = datetime.today() - timedelta(days=1)
date_query = yesterday.strftime('%Y-%m-%d')

#get funnels events data
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()

mycursor.execute('SELECT appmetrica_device_id,event_name,os_name,event_datetime FROM events WHERE event_name IN \
                 ("product_opened","product_added_to_cart","open_cart","order_reg","open_pay_button","order_paid") AND event_datetime = "' + date_query + '"')

events_funnels_buyers_list = mycursor.fetchall()
db_connection.close()

#get funnels events data
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()

mycursor.execute('SELECT appmetrica_device_id,event_name,os_name,event_datetime FROM events WHERE event_name IN \
                  ("create_start","create_brand_choose","product_created","order_confirmed_availability") AND event_datetime = "' + date_query + '"')

events_funnels_created_list = mycursor.fetchall()
db_connection.close()

events_funnels_buyers_df = pd.DataFrame(events_funnels_buyers_list)
events_funnels_buyers_df.columns = ['appmetrica_device_id','event_name','os_name','event_datetime']

events_funnels_created_df = pd.DataFrame(events_funnels_created_list)
events_funnels_created_df.columns = ['appmetrica_device_id','event_name','os_name','event_datetime']

#add exp parameter
#mysql connection
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()

#get purchases id
mycursor.execute('SELECT appmetrica_device_id FROM purchases')
purchases_id_list = mycursor.fetchall()
purchases_id_df = pd.DataFrame(purchases_id_list)
purchases_id_df.columns = ['appmetrica_device_id']
purchases_id_df = purchases_id_df.drop_duplicates(subset=['appmetrica_device_id'])
purchases_id_list = purchases_id_df.appmetrica_device_id.tolist()

#get created id
mycursor.execute('SELECT appmetrica_device_id FROM events WHERE event_name = "product_created"')
created_id_list = mycursor.fetchall()
created_id_df = pd.DataFrame(created_id_list)
created_id_df.columns = ['appmetrica_device_id']
created_id_df = created_id_df.drop_duplicates(subset=['appmetrica_device_id'])
created_id_list = created_id_df.appmetrica_device_id.tolist()

db_connection.close()

#add exp to df
created_exp = events_funnels_created_df.appmetrica_device_id.isin(created_id_df.appmetrica_device_id)
events_funnels_created_df['exp'] = created_exp
events_funnels_created_list_all = events_funnels_created_df.values.tolist()

buyers_exp = events_funnels_buyers_df.appmetrica_device_id.isin(purchases_id_df.appmetrica_device_id)
events_funnels_buyers_df['exp'] = buyers_exp
events_funnels_buyers_list_all = events_funnels_buyers_df.values.tolist()

#write to mysql
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()

col_names = list(events_funnels_buyers_df.columns.values)
col_names = ','.join(col_names)
col_names_index = np.repeat('%s', 5, axis=0)
col_names_index = ','.join(col_names_index)
sql_query_created = 'INSERT INTO funnels_events_buyers (' + col_names + ') VALUES (' + col_names_index + ')'
mycursor.executemany(sql_query_created,events_funnels_buyers_list_all)
db_connection.commit()
db_connection.close()

db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()
sql_query_created = 'INSERT INTO funnels_events_created (' + col_names + ') VALUES (' + col_names_index + ')'
mycursor.executemany(sql_query_created,events_funnels_created_list_all)
db_connection.commit()
db_connection.close()
