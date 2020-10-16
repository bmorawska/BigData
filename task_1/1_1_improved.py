import time
import pandas as pd
from task_1.path import PATH_TO_FILE

df = pd.read_csv(PATH_TO_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'], dtype={"Agency Name": "category",
                                                                                            "Complaint Type": "category",
                                                                                            "Borough": "category"})
print(df.memory_usage(index=False, deep=True).sum())
print(df.dtypes)

# Najczęściej zgłaszane skargi
start_time_query_1 = time.perf_counter()
query_1 = df['Complaint Type'].value_counts().head(1)
end_time_query_1 = time.perf_counter()

print('-------najczęściej zgłaszane skargi-------')
print(query_1)
print('\nCzas: ', end_time_query_1 - start_time_query_1)
print('------------------------------------------')

# Najczęściej zgłaszane skargi w każdej dzielnicy

start_time_query_2 = time.perf_counter()
query_2 = df.groupby(['Borough'])['Complaint Type'].agg(Complaint_Type=pd.Series.mode,
                                                        Complaint_Count=lambda x: x.value_counts().head(1))

end_time_query_2 = time.perf_counter()

print('\n-------najczęściej zgłaszane skargi w każdej dzielnicy-------')
print(query_2)
print('\nCzas: ', end_time_query_2 - start_time_query_2)
print('------------------------------------------------------------')

# Urzędy, do których najczęściej zgłaszano skargi
start_time_query_3 = time.perf_counter()
query_3 = df['Agency Name'].value_counts().head(1)
end_time_query_3 = time.perf_counter()

print('\n-------urzędy, do których najczęściej zgłaszano skargi-------')
print(query_3)
print('\nCzas: ', end_time_query_3 - start_time_query_3)
print('-------------------------------------------------------------')

"""
WYNIKI

-------najczęściej zgłaszane skargi-------
Noise - Residential    2139470
Name: Complaint Type, dtype: int64

Czas:  0.15980715799999246
------------------------------------------

-------najczęściej zgłaszane skargi w każdej dzielnicy-------
                                                  Complaint_Type                                    Complaint_Count
Borough                                                                                                            
BRONX                                        Noise - Residential                                             575361
BROOKLYN       0    Noise - Residential
Name: Complaint Type,...  Noise - Residential    617491
Name: Complaint ...
MANHATTAN      0    Noise - Residential
Name: Complaint Type,...  Noise - Residential    458422
Name: Complaint ...
QUEENS         0    Noise - Residential
Name: Complaint Type,...  Noise - Residential    421524
Name: Complaint ...
STATEN ISLAND  0    Street Condition
Name: Complaint Type, dt...  Street Condition    125674
Name: Complaint Typ...
Unspecified    0    HEATING
Name: Complaint Type, dtype: cate...  HEATING    282916
Name: Complaint Type, dtype:...
Czas:  2.052715145999997
------------------------------------------------------------

-------urzędy, do których najczęściej zgłaszano skargi-------
New York City Police Department    6308385
Name: Agency Name, dtype: int64

Czas:  0.1672891719999967
-------------------------------------------------------------

Memory - 121028217 bytes = ~0.11 gigabytes
"""
