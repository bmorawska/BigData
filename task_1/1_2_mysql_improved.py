import time
import pandas as pd
import mysql.connector
from task_1.path import PATH_TO_FILE, PATH_TO_EXAMPLE_FILE

"""
Czas tworzenia bazy danych i tabeli (wykonanie pliku crating_mysql_DB.sql): 
18:25:43	CREATE DATABASE randomDB	1 row(s) affected	0.015 sec
18:25:43	USE randomDB	            0 row(s) affected	0.000 sec
18:25:43	CREATE TABLE [...]	        0 row(s) affected	0.063 sec
"""

start_database_insert_data = time.perf_counter()

conn = mysql.connector.connect(db='randomDB', host='localhost', user='root', password='123')
db_cursor = conn.cursor()

insert_data_query = "INSERT INTO SERVICE_REQUESTS(AGENCY_NAME, COMPLAINT_TYPE, BOROUGH) VALUES(%s, %s, %s)"

for chunk in pd.read_csv(PATH_TO_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'],
                         chunksize=10000,
                         keep_default_na=False,
                         dtype={"Agency Name": "category",
                                "Complaint Type": "category",
                                "Borough": "category"}
                         ):

    data = list(zip(chunk['Agency Name'].to_numpy(), chunk['Complaint Type'].to_numpy(), chunk['Borough'].to_numpy()))

    try:
        db_cursor.executemany(insert_data_query, data)
        conn.commit()
        ## print('success')

    except (mysql.connector.Error, mysql.connector.Warning) as e:
        conn.close()
        print(e)

end_database_insert_data = time.perf_counter()

print('-------czas wpuszczania danych do bazy mysql-------')
print('Czas: ', end_database_insert_data - start_database_insert_data)
print('------------------------------------------')

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

print('\n-------najczęściej zgłaszane skargi-------')
print(df_query1)
print('\nCzas: ', end_time_query_1 - start_time_query_1)
print('------------------------------------------')

# Najczęściej składane skargi w każdej dzielnicy
start_time_query_2 = time.perf_counter()

"""
query_2 = "SELECT t.borough, t.complaint_type, MAX(t.total) AS complaint_count " \
          "FROM (" \
          "SELECT borough, complaint_type, COUNT(complaint_type) AS total " \
          "FROM service_requests " \
          "GROUP BY borough, complaint_type) AS t " \
          "GROUP BY t.borough"
          """

query_2 = "SELECT t.borough, (SELECT complaint_type " \
          "FROM service_requests " \
          "WHERE borough = t.borough " \
          "GROUP BY complaint_type " \
          "ORDER BY COUNT(*) DESC " \
          "LIMIT 1) AS complaint_type " \
          "FROM (SELECT DISTINCT borough FROM service_requests) AS t"

db_cursor.execute(query_2)
end_time_query_2 = time.perf_counter()

df_query2 = pd.DataFrame(db_cursor.fetchall())

print('\n-------najczęściej zgłaszane skargi w każdej dzielnicy-------')
print(df_query2)
print('\nCzas: ', end_time_query_2 - start_time_query_2)
print('------------------------------------------------------------')


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

conn.close()

"""
WYNIKI

-------czas wpuszczania danych do bazy mysql-------
Czas:  853.6806388379999
------------------------------------------

-------najczęściej zgłaszane skargi-------
                     0        1
0  Noise - Residential  2139470

Czas:  123.15957107000008
------------------------------------------

-------najczęściej zgłaszane skargi w każdej dzielnicy-------
               0                    1
0       BROOKLYN  Noise - Residential
1         QUEENS  Noise - Residential
2  STATEN ISLAND     Street Condition
3      MANHATTAN  Noise - Residential
4    Unspecified              HEATING
5          BRONX  Noise - Residential

Czas:  115.09215386999995
------------------------------------------------------------

-------urzędy, do których najczęściej zgłaszano skargi-------
                                 0        1
0  New York City Police Department  6308385

Czas:  148.75368063400015
-------------------------------------------------------------

"""
