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
host = '95.181.198.34'
user = 'oleg_user'
passwd = 'kz59qj07w'
database = 'appmetrica'
table = 'attribution'

#mysql connection
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()

#query to mysql
yesterday = datetime.today() - timedelta(days=1)
date_query = yesterday.strftime('%Y-%m-%d')

mycursor.execute('SELECT appmetrica_device_id,event_name,os_name,event_datetime FROM events WHERE event_name IN \
                 ("product_opened","product_added_to_cart","open_cart","order_reg","open_pay_button","order_paid",\
                  "create_start","create_brand_choose","product_created","order_confirmed_availability") AND \
                event_datetime = "' + date_query + '"')

events_list = mycursor.fetchall()
db_connection.close()

events_funnels_df = pd.DataFrame(events_list)
events_funnels_df.columns = ['appmetrica_device_id','event_name','os_name','event_datetime']
events_funnels_list_all = events_funnels_df.values.tolist()

#mysql connection
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()
#write data to mysql
col_names = list(events_funnels_df.columns.values)
col_names = ','.join(col_names)
col_names_index = np.repeat('%s', 4, axis=0)
col_names_index = ','.join(col_names_index)
sql_query_created = 'INSERT INTO funnels_events (' + col_names + ') VALUES (' + col_names_index + ')'
mycursor.executemany(sql_query_created,events_funnels_list_all)
db_connection.commit()
db_connection.close()