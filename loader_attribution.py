# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 10:16:16 2020

@author: web
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
mycursor.execute('SELECT install_datetime FROM attribution WHERE install_datetime IN ("' + yesterday_ymd + '") ORDER BY install_datetime')
date_list = mycursor.fetchall()
if len(date_list) == 0:
    dates = pd.Series(yesterday_ymd)
    counter = 0
    while len(date_list) == 0:
        counter += 1
        date_iter = yesterday - timedelta(days=counter)
        date_iter = date_iter.strftime('%Y-%m-%d')
        mycursor.execute('SELECT install_datetime FROM attribution WHERE install_datetime IN ("' + date_iter + '") ORDER BY install_datetime')
        date_list = mycursor.fetchall()
        if len(date_list) == 0:
            date_iter_ser = pd.Series(date_iter)
            dates = dates.append(date_iter_ser, ignore_index=True)            
else:    
    sys.exit('yesterday data is loaded')
    
#query to appmetrica logs api
#dates_add = pd.Series(['2020-12-12','2020-12-11'])
#dates = dates.append(dates_add, ignore_index=True)

date_since = min(dates)
date_until = max(dates)

#get data
data_installations = def_test_api.logs_api_export(app_id = 3188596, 
                             date_since = date_since, date_until = date_until,
                             type_report = 'installations', 
                             fields = 'appmetrica_device_id,tracker_name,os_name,install_datetime')

data_installations_csv = StringIO(data_installations)
data_installations_df = pd.read_csv(data_installations_csv, sep=",")

#convert date formar y-m-d h-m-s to y-m-d
data_installations_df['install_datetime'] = pd.to_datetime(data_installations_df['install_datetime'],format = '%Y-%m-%d %H:%M:%S')
data_installations_df['install_datetime'] = data_installations_df['install_datetime'].dt.strftime('%Y-%m-%d')

data_installations_list = data_installations_df.values.tolist()
data_installations_str = str(data_installations_df)

with open('text.txt', 'w') as text_file:
    text_file.write(data_installations_str)    
text_file.close()

#write new data to mysql
#sql_query = "INSERT INTO attribution (appmetrica_device_id,tracker_name,os_name,install_datetime) VALUES (%s, %s, %s, %s)"
#mycursor.executemany(sql_query,data_installations_list)
#db_connection.commit()
#db_connection.close()
