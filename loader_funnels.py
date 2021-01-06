# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 15:48:13 2020

@author: web
"""

import mysql.connector
import  pandas as pd
from datetime import date, timedelta, datetime
import numpy as np


def create_funn_buyers (iter_df, exp_type:chr, funn_type:chr):
    #create funnels df
    events_gr_df = iter_df.groupby(['event_datetime','event_name','appmetrica_device_id'],axis = 0)['os_name'].count()
    iter_ser = pd.Series([date_query,
                  exp_type,
                  funn_type,
                  len(iter_df.appmetrica_device_id.unique()),         
                  len(events_gr_df.filter(like = 'product_opened')),         
                  len(events_gr_df.filter(like = 'product_added_to_cart')),
                  len(events_gr_df.filter(like = 'open_cart')),
                  len(events_gr_df.filter(like = 'order_reg')),
                  len(events_gr_df.filter(like = 'open_pay_button')),
                  len(events_gr_df.filter(like = 'order_paid'))])
    iter_ser = pd.DataFrame(iter_ser)
    iter_ser = iter_ser.transpose()
    iter_ser.columns = ['date','buy_exp','type_funnel',
                        'unique_devices','product_opened','product_added_to_cart','open_cart','order_reg','open_pay_button','order_paid']
    return(iter_ser)

def create_funn_created (iter_df, exp_type:chr, funn_type:chr):
    #create funnels df
    events_gr_df = iter_df.groupby(['event_datetime','event_name','appmetrica_device_id'],axis = 0)['os_name'].count()
    iter_ser = pd.Series([date_query,
                  exp_type,
                  funn_type,
                  len(iter_df.appmetrica_device_id.unique()),         
                  len(events_gr_df.filter(like = 'create_start')),         
                  len(events_gr_df.filter(like = 'create_brand_choose')),
                  len(events_gr_df.filter(like = 'product_created')),
                  len(events_gr_df.filter(like = 'order_confirmed_availability'))])
    iter_ser = pd.DataFrame(iter_ser)
    iter_ser = iter_ser.transpose()
    iter_ser.columns = ['date','created_exp','type_funnel',
                        'unique_devices','create_start','create_brand_choose','product_created','order_confirmed_availability']
    return(iter_ser)

#connection params
host = 'localhost'
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

#funnels
yesterday = datetime.today() - timedelta(days=1)
date_query = yesterday.strftime('%Y-%m-%d')
    
db_connection = mysql.connector.connect(
        host=host,
        user=user,
        passwd=passwd,
        database=database
)
mycursor = db_connection.cursor()
    
mycursor.execute('SELECT * FROM events WHERE event_datetime = "' + date_query + '"')
events_list = mycursor.fetchall()
db_connection.close()
events_df0 = pd.DataFrame(events_list, columns = ['appmetrica_device_id','event_name','event_json','os_name','event_datetime'])
    
events_df = events_df0
    
funn_mod_df = events_df[(events_df.event_name.isin(['open_cart','order_reg','open_pay_button','order_paid']))]
funn_mod_df = funn_mod_df.reset_index(drop = True)
funn_mod_set = set(funn_mod_df.appmetrica_device_id)
funn_id_df = events_df[(events_df.event_name == 'product_added_to_cart')]
funn_id_df = funn_id_df.reset_index(drop = True)
funn_id_set = set(funn_id_df.appmetrica_device_id)
funn_diff_df = funn_mod_set.difference(funn_id_set)

funn_diff_df = list(funn_diff_df)
funn_mod_df = funn_mod_df[~funn_mod_df.appmetrica_device_id.isin(funn_diff_df)]
    
events_df = events_df[~events_df.event_name.isin(['open_cart','order_reg','open_pay_button','order_paid'])]
events_df = events_df.append(funn_mod_df, ignore_index=True)
    
#group by exp
events_buyers_yes_exp_df = events_df[events_df.appmetrica_device_id.isin(purchases_id_list)]
events_buyers_no_exp_df = events_df[~events_df.appmetrica_device_id.isin(purchases_id_list)]
events_created_yes_exp_df = events_df[events_df.appmetrica_device_id.isin(created_id_list)]
events_created_no_exp_df = events_df[~events_df.appmetrica_device_id.isin(created_id_list)]
    
lisf_events_buyers = [events_buyers_yes_exp_df, events_buyers_no_exp_df]
lisf_events_created = [events_created_yes_exp_df, events_created_no_exp_df]
    
funnel_buyers = pd.DataFrame()    
for idx in range(len(lisf_events_buyers)):
    iter_df = lisf_events_buyers[idx]
    if idx == 0:
        exp_type = 'yes'
        funn_type = 'buyers'
        iter_ser = create_funn_buyers(iter_df, exp_type, funn_type)
        funnel_buyers = funnel_buyers.append(iter_ser, ignore_index=True)
    elif idx == 1:
        exp_type = 'no'
        funn_type = 'buyers'
        iter_ser = create_funn_buyers(iter_df, exp_type, funn_type)
        funnel_buyers = funnel_buyers.append(iter_ser, ignore_index=True)            
        
funnel_created = pd.DataFrame()
for idx in range(len(lisf_events_created)):
    iter_df = lisf_events_created[idx]
    if idx == 0:
        exp_type = 'yes'
        funn_type = 'created'
        iter_ser = create_funn_created(iter_df, exp_type, funn_type)
        funnel_created = funnel_created.append(iter_ser, ignore_index=True)
    elif idx == 1:
        exp_type = 'no'
        funn_type = 'created'
        iter_ser = create_funn_created(iter_df, exp_type, funn_type)
        funnel_created = funnel_created.append(iter_ser, ignore_index=True)
            
funnel_buyers_list = funnel_buyers.values.tolist()
funnel_created_list = funnel_created.values.tolist()
    
col_names_buyers = list(funnel_buyers.columns.values)
col_names_buyers = ','.join(col_names_buyers)
col_names_index_buyers = np.repeat('%s', len(funnel_buyers.columns), axis=0)
col_names_index_buyers = ','.join(col_names_index_buyers)
    
db_connection = mysql.connector.connect(
        host=host,
        user=user,
        passwd=passwd,
        database=database
)
mycursor = db_connection.cursor()
    
sql_query_buyers = 'INSERT INTO funnels_buyers (' + col_names_buyers + ') VALUES (' + col_names_index_buyers + ')'
mycursor.executemany(sql_query_buyers,funnel_buyers_list)
db_connection.commit()
    
col_names_created = list(funnel_created.columns.values)
col_names_created = ','.join(col_names_created)
col_names_index_created = np.repeat('%s', len(funnel_created.columns), axis=0)
col_names_index_created = ','.join(col_names_index_created)
    
sql_query_created = 'INSERT INTO funnels_created (' + col_names_created + ') VALUES (' + col_names_index_created + ')'
mycursor.executemany(sql_query_created,funnel_created_list)
db_connection.commit()
db_connection.close()
    
