import sqlite3 as sql
import pandas as pd
import time

from task_1.path import PATH_TO_FILE, PATH_TO_EXAMPLE_FILE

# Wersja nieoszczędzająca RAMu, tworzy bazę szybciej, ale trzeba wczytać 12GB do pamięci i nie każdy komputer temu
# podoła (mój laptop odmówił :-P). Czas działania programu jest taki sam jak w 1_2_v2.py

data = pd.read_csv(PATH_TO_FILE)
data.columns = data.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
# Jak tego na górze nie ma to pluje brzydkim, czerwonym komunikatem, więc lepiej jak jest. Służy to żeby zamienić
# nazwy kolumn typu Big Data Jest Super na big_data_jest_super. Biblioteka pandas woli takie nazwy niż
# duże litery i spacje. Tylko trzeba to uwzględnić w nazwach kolumn w kwerendach.

conn = sql.connect('incidents.db')
try:
    data.to_sql('incidents', conn, if_exists='fail')
except ValueError:
    print('\nBaza danych już istnieje, więc nie tworzę nowej.\n')

start_time_query = time.perf_counter()
query = pd.read_sql('SELECT "complaint_type", COUNT("complaint_type") AS "frequency" '
                    'FROM incidents  '
                    'GROUP BY "complaint_type" '
                    'ORDER BY COUNT(*) DESC '
                    'LIMIT 5', conn)

end_time_query = time.perf_counter()

print('-------najczęściej zgłaszane skargi-------')
print(query)
print('\nCzas: ', end_time_query - start_time_query)
print('------------------------------------------')
