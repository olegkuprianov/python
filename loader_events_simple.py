# -*- coding: utf-8 -*-
"""
Created on Sun Dec 20 16:38:43 2020

@author: oksan
"""

import mysql.connector
from datetime import datetime
from datetime import timedelta
import  pandas as pd
import def_test_api
from io import StringIO

yesterday = datetime.today() - timedelta(days=1)
yesterday_ymd = yesterday.strftime('%Y-%m-%d')

date_since = yesterday_ymd
date_until = yesterday_ymd

#get data from appmetrica logs api
data_events = def_test_api.logs_api_export(app_id = 3188596, 
                             date_since = date_since, date_until = date_until,
                             type_report = 'events', 
                             fields = 'appmetrica_device_id,event_name,event_json,os_name,event_datetime')

data_events_csv = StringIO(data_events)
data_events_df = pd.read_csv(data_events_csv, sep=",")

#convert date formar y-m-d h-m-s to y-m-d
data_events_df['event_datetime'] = pd.to_datetime(data_events_df['event_datetime'],format = '%Y-%m-%d %H:%M:%S')
data_events_df['event_datetime'] = data_events_df['event_datetime'].dt.strftime('%Y-%m-%d')
data_events_df = data_events_df.fillna(0)

test = str(data_events_df['event_json'])
test1 = test.encode("utf-8").strip()[:65535]
data_events_df['event_json'] = test1

data_events_list = data_events_df.values.tolist()
#data_events_str = str(data_events_df)

#with open('text.txt', 'w') as text_file:
    #text_file.write(data_events_str)
#text_file.close()

#connection params
host = 'localhost'
user = 'oleg_user'
passwd = 'kz59qj07w'
database = 'appmetrica'
table = 'attribution'

#create connection
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database,
  use_unicode=True,
  charset='utf8'
)
mycursor = db_connection.cursor()

#write new data to mysql
sql_query = "INSERT INTO events (appmetrica_device_id,event_name,event_json,os_name,event_datetime) VALUES (%s, %s, %s, %s, %s)"
mycursor.executemany(sql_query,data_events_list)
db_connection.commit()
db_connection.close()


