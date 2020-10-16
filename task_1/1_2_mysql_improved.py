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

"""
-------czas wpuszczania danych do bazy mysql-------

Czas:  4220.1825219
---------------------------------------------------


-------czas wpuszczania danych do bazy mysql-------

Czas:  967.5188049120001
------------------------------------------
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

print('\n-------najczęściej zgłaszane skargi-------')
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


-------najczęściej zgłaszane skargi-------
                     0        1
0  Noise - Residential  1626400

Czas:  97.41855300799989
------------------------------------------
"""

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


-------najczęściej zgłaszane skargi w każdej dzielnicy-------
               0                       1       2
0      MANHATTAN      Noise - Commercial  349183
1       BROOKLYN      Sidewalk Condition  468032
2          BRONX               APPLIANCE  447610
3         QUEENS        Dirty Conditions  311845
4  STATEN ISLAND         Dead/Dying Tree  104573
5    Unspecified  Street Light Condition  282916
6                    Noise - Residential       4

Czas:  64.63014332499984
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


-------urzędy, do których najczęściej zgłaszano skargi-------
                                                   0        1
0  Department of Housing Preservation and Develop...  5197486

Czas:  91.393568254
-------------------------------------------------------------

"""

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
