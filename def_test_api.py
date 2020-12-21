# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 14:24:48 2020

@author: web
"""

import requests
import  pandas as pd
from io import StringIO
import time

def logs_api_export(app_id: str, 
                        date_since: chr, date_until: chr, 
                        type_report: chr, fields: chr):
    url = 'https://api.appmetrica.yandex.ru/logs/v1/export/' + type_report +'.csv'
    headers = {
            'User-Agent': 'appmetrica-logsapi-loader/0.1.1',
            'Accept-Encoding': 'gzip',
            'Authorization': 'OAuth AgAAAAA8yU0pAAa-h2XRbvB05k3lhycXiEKvE58'
        }
    params = {
            'application_id': app_id,
            'fields': fields,
            'date_since': date_since,
            'date_until': date_until
        }
    response = requests.get(url, params=params,headers=headers,stream=True)
    while response.status_code != 200:
        time.sleep(5)
        response = requests.get(url, params=params,headers=headers,stream=True)        

    #string_get = response.text
    response_string = response.content.decode('utf8')
    return response_string

