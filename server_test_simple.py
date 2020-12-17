# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 16:00:46 2020

@author: web

server folders

test_py/test_1

"""
#import mysql.connector
from datetime import datetime
from datetime import timedelta

today = datetime.today()
today = today.strftime('%Y-%m-%d %H:%M:%S')

phrase = 'Hello! Test_1'

with open('text.txt', 'w') as text_file:
    text_file.write(today)
    
text_file.close()
