import time
import pandas as pd
from task_1.path import PATH_TO_FILE

df = pd.read_csv(PATH_TO_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'])
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

Czas:  2.034412314000008
------------------------------------------

-------najczęściej zgłaszane skargi w każdej dzielnicy-------
                    Complaint_Type  Complaint_Count
Borough                                            
BRONX          Noise - Residential           575361
BROOKLYN       Noise - Residential           617491
MANHATTAN      Noise - Residential           458422
QUEENS         Noise - Residential           421524
STATEN ISLAND     Street Condition           125674
Unspecified                HEATING           282916

Czas:  6.500596621
------------------------------------------------------------

-------urzędy, do których najczęściej zgłaszano skargi-------
New York City Police Department    6308385
Name: Agency Name, dtype: int64

Czas:  1.9665533730000107
-------------------------------------------------------------

Memory - 5550556549 bytes = ~5.17 gigabytes
"""