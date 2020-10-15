import time
import pandas as pd
import mysql.connector
from task_1.path import PATH_TO_FILE, PATH_TO_EXAMPLE_FILE

"""
Czas tworzenia bazy danych i tabeli (wykonanie pliku crating_mysql_DB.sql): 
12:13:57	CREATE DATABASE randomDB	1 row(s) affected	0.141 sec
12:13:57	USE randomDB	            0 row(s) affected	0.000 sec
12:13:57	CREATE TABLE [...]	        0 row(s) affected	1.094 sec
"""

start_database_insert_data = time.perf_counter()

conn = mysql.connector.connect(db='randomDB', host='localhost', user='root', password='123')
db_cursor = conn.cursor()

insert_data_query = "INSERT INTO SERVICE_REQUESTS(AGENCY_NAME, COMPLAINT_TYPE, BOROUGH) VALUES(%s, %s, %s)"

for chunk in pd.read_csv(PATH_TO_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'], chunksize=10000):

    data = list(zip(chunk['Agency Name'], chunk['Complaint Type'], chunk['Borough']))

    try:
        db_cursor.executemany(insert_data_query, data)
        conn.commit()
        print('success')

    except (mysql.connector.Error, mysql.connector.Warning) as e:
        conn.close()
        print(e)

end_database_insert_data = time.perf_counter()

print('-------czas wpuszczania danych do bazy mysql-------')
print('\nCzas: ', end_database_insert_data - start_database_insert_data)
print('------------------------------------------')

"""
-------czas wpuszczania danych do bazy mysql-------

Czas:  4220.1825219
---------------------------------------------------
"""

# Najczęściej składane skargi
start_time_query_1 = time.perf_counter()

query_1 = "SELECT complaint_type, COUNT(complaint_type) AS frequency " \
          "FROM service_requests " \
          "GROUP BY complaint_type " \
          "ORDER BY COUNT(*) DESC " \
          "LIMIT 1"

db_cursor.execute(query_1)

end_time_query_1 = time.perf_counter()
df_query1 = pd.DataFrame(db_cursor.fetchall())

print('-------najczęściej zgłaszane skargi-------')
print(df_query1)
print('\nCzas: ', end_time_query_1 - start_time_query_1)
print('------------------------------------------')

"""
-------najczęściej zgłaszane skargi-------
0  Noise - Residential  2128035
Czas:  71.9516774
------------------------------------------
-------najczęściej zgłaszane skargi-------
[...]
Czas:  130.62204229999952
------------------------------------------
"""

# Najczęściej składane skargi w każdej dzielnicy
start_time_query_2 = time.perf_counter()

query_2 = "SELECT t.borough, t.complaint_type, MAX(t.total) AS complaint_count " \
          "FROM (" \
          "SELECT borough, complaint_type, COUNT(complaint_type) AS total " \
          "FROM service_requests " \
          "GROUP BY borough, complaint_type) AS t " \
          "GROUP BY t.borough"

db_cursor.execute(query_2)
end_time_query_2 = time.perf_counter()

df_query2 = pd.DataFrame(db_cursor.fetchall())

print('\n-------najczęściej zgłaszane skargi w każdej dzielnicy-------')
print(df_query2)
print('\nCzas: ', end_time_query_2 - start_time_query_2)
print('------------------------------------------------------------')

"""
-------najczęściej zgłaszane skargi w każdej dzielnicy-------
0         QUEENS           Rodent  419169
1  STATEN ISLAND            Sewer  125556
2       BROOKLYN  Illegal Parking  614950
3      MANHATTAN         Elevator  456246
4          BRONX           Rodent  571301
5    Unspecified  Taxi Compliment  282916

Czas:  77.7641523
------------------------------------------------------------

-------najczęściej zgłaszane skargi w każdej dzielnicy-------
[...]
Czas:  126.9398699000003
------------------------------------------------------------

"""

# Urzędy, do których najczęściej zgłaszano skargi
start_time_query_3 = time.perf_counter()

query_3 = "SELECT agency_name, COUNT(agency_name) AS frequency " \
          "FROM service_requests " \
          "GROUP BY agency_name " \
          "ORDER BY COUNT(*) DESC " \
          "LIMIT 1"

db_cursor.execute(query_3)
end_time_query_3 = time.perf_counter()

df_query3 = pd.DataFrame(db_cursor.fetchall())

print('\n-------urzędy, do których najczęściej zgłaszano skargi-------')
print(df_query3)
print('\nCzas: ', end_time_query_3 - start_time_query_3)
print('-------------------------------------------------------------')

"""
-------urzędy, do których najczęściej zgłaszano skargi-------
0  New York City Police Department  6276840
Czas:  73.6223479
-------------------------------------------------------------
-------urzędy, do których najczęściej zgłaszano skargi-------
[...]
Czas:  102.4953972000003
-------------------------------------------------------------
"""

conn.close()
