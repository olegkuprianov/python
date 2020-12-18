import def_test_api

date_since = '2020-12-16'
date_until = '2020-12-17'

#get data
data_installations = def_test_api.logs_api_export(app_id = 3188596, 
                             date_since = date_since, date_until = date_until,
                             type_report = 'installations', 
                             fields = 'appmetrica_device_id,tracker_name,os_name,install_datetime')

data_installations = str(data_installations)

with open('text.txt', 'w') as text_file:
    text_file.write(data_installations)
    
text_file.close()
