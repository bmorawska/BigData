import sqlite3 as sql
import pandas as pd
import time

from task_1.path import PATH_TO_FILE, PATH_TO_EXAMPLE_FILE

data = pd.read_csv(PATH_TO_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'])
data.columns = data.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
# Jak tego na górze nie ma to pluje brzydkim, czerwonym komunikatem, więc lepiej jak jest. Służy to żeby zamienić
# nazwy kolumn typu Big Data Jest Super na big_data_jest_super. Biblioteka pandas woli takie nazwy niż
# duże litery i spacje. Tylko trzeba to uwzględnić w nazwach kolumn w kwerendach.

conn = sql.connect('incidents.db')
try:
    data.to_sql('incidents', conn, if_exists='fail')
except ValueError:
    print('\nBaza danych już istnieje, więc nie tworzę nowej.\n')

start_time_query_1 = time.perf_counter()

query_1 = pd.read_sql('SELECT complaint_type, COUNT(complaint_type) AS frequency '
                      'FROM incidents '
                      'GROUP BY complaint_type '
                      'ORDER BY COUNT(*) DESC '
                      'LIMIT 1', conn)

end_time_query_1 = time.perf_counter()

print('-------najczęściej zgłaszane skargi-------')
print(query_1)
print('\nCzas: ', end_time_query_1 - start_time_query_1)
print('------------------------------------------')

"""
start_time_query_2 = time.perf_counter()

query_2 = pd.read_sql('SELECT t.borough, (SELECT complaint_type '
                      'FROM incidents '
                      'WHERE borough = t.borough '
                      'GROUP BY complaint_type '
                      'ORDER BY COUNT(*) DESC '
                      'LIMIT 1) AS complaint_type, '
                      '(SELECT COUNT(complaint_type) '
                      'FROM incidents '
                      'WHERE borough = t.borough '
                      'GROUP BY complaint_type '
                      'ORDER BY COUNT(*) DESC '
                      'LIMIT 1) AS complaint_count '
                      'FROM (SELECT DISTINCT borough FROM incidents) AS t', conn)
end_time_query_2 = time.perf_counter()

print('\n-------najczęściej zgłaszane skargi w każdej dzielnicy-------')
print(query_2)
print('\nCzas: ', end_time_query_2 - start_time_query_2)
print('------------------------------------------------------------')
"""

start_time_query_2 = time.perf_counter()
query_2 = pd.read_sql('SELECT t.borough, t.complaint_type, MAX(t.total) AS complaint_count '
                      'FROM (SELECT borough, complaint_type, COUNT(complaint_type) AS total '
                      'FROM incidents '
                      'GROUP BY borough, complaint_type) AS t '
                      'WHERE t.borough IS NOT NULL '
                      'GROUP BY t.borough', conn)

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
print('\nCzas: ', end_time_query_1 - start_time_query_1)
print('-------------------------------------------------------------')
