import sqlite3 as sql
import pandas as pd
import time

from task_1.path import PATH_TO_FILE, PATH_TO_EXAMPLE_FILE

start_database_read_data = time.perf_counter()

data = pd.read_csv(PATH_TO_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'], dtype={"Agency Name": "category",
                                                                                            "Complaint Type": "category",
                                                                                            "Borough": "category"})
data.columns = data.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
end_database_read_data = time.perf_counter()

print('-------czas wczytywania danych do pandas-------')
print('Czas: ', end_database_read_data - start_database_read_data)
print('------------------------------------------')

start_database_insert_data = time.perf_counter()

conn = sql.connect('incidents.db')
try:
    data.to_sql('incidents', conn, if_exists='fail')
except ValueError:
    print('\nBaza danych już istnieje, więc nie tworzę nowej.\n')

end_database_insert_data = time.perf_counter()

print('-------czas wpuszczania danych do bazy sqlite-------')
print('Czas: ', end_database_insert_data - start_database_insert_data)
print('------------------------------------------')

start_time_query_1 = time.perf_counter()

query_1 = pd.read_sql('SELECT complaint_type, COUNT(complaint_type) AS frequency '
                      'FROM incidents '
                      'GROUP BY complaint_type '
                      'ORDER BY COUNT(*) DESC '
                      'LIMIT 1', conn)

end_time_query_1 = time.perf_counter()

print('\n-------najczęściej zgłaszane skargi-------')
print(query_1)
print('\nCzas: ', end_time_query_1 - start_time_query_1)
print('------------------------------------------')

"""
start_time_query_2 = time.perf_counter()
query_2 = pd.read_sql('SELECT t.borough, t.complaint_type, MAX(t.total) AS complaint_count '
                      'FROM (SELECT borough, complaint_type, COUNT(complaint_type) AS total '
                      'FROM incidents '
                      'GROUP BY borough, complaint_type) AS t '
                      'WHERE t.borough IS NOT NULL '
                      'GROUP BY t.borough', conn)

end_time_query_2 = time.perf_counter()
"""

start_time_query_2 = time.perf_counter()

query_2 = pd.read_sql('SELECT t.borough, (SELECT complaint_type '
                      'FROM incidents '
                      'WHERE borough = t.borough '
                      'GROUP BY complaint_type '
                      'ORDER BY COUNT(*) DESC '
                      'LIMIT 1) AS complaint_type '
                      'FROM (SELECT DISTINCT borough FROM incidents) AS t', conn)
end_time_query_2 = time.perf_counter()

print('\n-------najczęściej zgłaszane skargi w każdej dzielnicy-------')
print(query_2)
print('\nCzas: ', end_time_query_2 - start_time_query_2)
print('------------------------------------------------------------')




# Urzędy, do których najczęściej zgłaszano skargi
start_time_query_3 = time.perf_counter()

query_3 = pd.read_sql('SELECT "agency_name", COUNT("agency_name") AS "frequency" '
                      'FROM incidents '
                      'GROUP BY "agency_name" '
                      'ORDER BY COUNT(*) DESC '
                      'LIMIT 1', conn)
end_time_query_3 = time.perf_counter()

print('\n-------urzędy, do których najczęściej zgłaszano skargi-------')
print(query_3)
print('\nCzas: ', end_time_query_3 - start_time_query_3)
print('-------------------------------------------------------------')

"""
WYNIKI

-------czas wczytywania danych do pandas-------
Czas:  114.830675631
------------------------------------------
-------czas wpuszczania danych do bazy sqlite-------
Czas:  58.275101112000016
------------------------------------------

-------najczęściej zgłaszane skargi-------
        complaint_type  frequency
0  Noise - Residential    2139470

Czas:  31.237601444000006
------------------------------------------

-------najczęściej zgłaszane skargi w każdej dzielnicy-------
         borough       complaint_type
0       BROOKLYN  Noise - Residential
1         QUEENS  Noise - Residential
2  STATEN ISLAND     Street Condition
3      MANHATTAN  Noise - Residential
4    Unspecified              HEATING
5          BRONX  Noise - Residential

Czas:  43.372081141999985
------------------------------------------------------------

-------urzędy, do których najczęściej zgłaszano skargi-------
                       agency_name  frequency
0  New York City Police Department    6308385

Czas:  22.094404105000024
-------------------------------------------------------------


"""