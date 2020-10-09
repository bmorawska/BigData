import sqlite3 as sql
import pandas as pd
import time
import os.path
from tqdm import tqdm  # wyświetla stan postępu

from task_1.path import PATH_TO_FILE, PATH_TO_EXAMPLE_FILE

# Wersja oszczędna dla RAMu, ale baza tworzy się (co prawda jednorazowo) ponad 20 minut.

conn = sql.connect('incidents.db')

if os.path.isfile('incidents.db'):
    print('\nBaza danych już istnieje, więc nie tworzę nowej.\n')
else:
    chunksize = 10 ** 6
    for chunk in tqdm(pd.read_csv(PATH_TO_FILE, chunksize=chunksize, low_memory=False)):
        chunk.columns = chunk.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')',                                                                                                             '')
        # Jak tego na górze nie ma to pluje brzydkim, czerwonym komunikatem, więc lepiej jak jest. Służy to żeby zamienić
        # nazwy kolumn typu Big Data Jest Super na big_data_jest_super. Biblioteka pandas woli takie nazwy niż
        # duże litery i spacje. Tylko trzeba to uwzględnić w nazwach kolumn w kwerendach.
        chunk.to_sql('incidents', conn, if_exists='append')

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



"""
U mnie na komputerze daje raczej różne wyniki...

najczęściej jest mniej więcej tak:

25it [22:56, 55.07s/it]
-------najczęściej zgłaszane skargi-------
        complaint_type  frequency
0  Noise - Residential    2128035
1       HEAT/HOT WATER    1323873
2      Illegal Parking    1054946
3     Street Condition    1006620
4     Blocked Driveway    1002038

Czas:  60.15288639999994
------------------------------------------

-------najczęściej zgłaszane skargi-------
[...]
Czas:  71.7015856
------------------------------------------

-------najczęściej zgłaszane skargi-------
[...]
Czas:  55.695587499999995
------------------------------------------

-------najczęściej zgłaszane skargi-------
[...]
Czas:  57.982712400000004
------------------------------------------

zdarzyło się też tak, ale tylko raz...
-------najczęściej zgłaszane skargi-------
[...]
Czas:  282.0429705
------------------------------------------

"""