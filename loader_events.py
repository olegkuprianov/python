# -*- coding: utf-8 -*-
"""
Created on Sat Dec 19 22:32:41 2020

@author: oksan
"""

import mysql.connector
from datetime import datetime
from datetime import timedelta
import sys
import  pandas as pd
import def_test_api
from io import StringIO

#connection params
host = '95.181.198.34'
user = 'oleg_user'
passwd = 'kz59qj07w'
database = 'appmetrica'
table = 'attribution'

#create connection
db_connection = mysql.connector.connect(
  host=host,
  user=user,
  passwd=passwd,
  database=database
)
mycursor = db_connection.cursor()

#check last date in table
yesterday = datetime.today() - timedelta(days=1)
yesterday_ymd = yesterday.strftime('%Y-%m-%d')
mycursor.execute('SELECT event_datetime FROM events WHERE event_datetime IN ("' + yesterday_ymd + '") ORDER BY event_datetime DESC LIMIT 100')
date_list = mycursor.fetchall()
if len(date_list) == 0:
    dates = pd.Series(yesterday_ymd)
    counter = 0
    while len(date_list) == 0:
        counter += 1
        date_iter = yesterday - timedelta(days=counter)
        date_iter = date_iter.strftime('%Y-%m-%d')
        mycursor.execute('SELECT event_datetime FROM events WHERE event_datetime IN ("' + date_iter + '") ORDER BY event_datetime DESC LIMIT 100')
        date_list = mycursor.fetchall()
        if len(date_list) == 0:
            date_iter_ser = pd.Series(date_iter)
            dates = dates.append(date_iter_ser, ignore_index=True)            
else:    
    sys.exit('yesterday data is loaded')
    
date_since = min(dates)
date_until = max(dates)

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

data_events_list = data_events_df.values.tolist()
data_events_str = str(data_events_df)

with open('text.txt', 'w') as text_file:
    text_file.write(data_events_str)
text_file.close()

#write new data to mysql
#sql_query = "INSERT INTO events (appmetrica_device_id,event_name,event_json,os_name,event_datetime) VALUES (%s, %s, %s, %s, %s)"
#mycursor.executemany(sql_query,data_events_list)
#db_connection.commit()
db_connection.close()











