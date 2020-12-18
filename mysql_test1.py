import mysql.connector

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
  database=database
)
mycursor = db_connection.cursor()

mycursor.execute('SELECT install_datetime FROM attribution WHERE install_datetime IN ("2020-12-01") ORDER BY install_datetime')
date_list = mycursor.fetchall()
date_str = str(date_list)

with open('text.txt', 'w') as text_file:
    text_file.write(date_str)
    
text_file.close()
