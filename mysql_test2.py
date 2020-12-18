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

sql = "INSERT INTO server_test (id, id1) VALUES (%s, %s)"
val = ("test_server","test_server1")
mycursor.execute(sql, val)
db_connection.commit()
db_connection.close()
