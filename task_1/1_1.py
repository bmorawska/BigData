import time
import pandas as pd
from task_1.path import PATH_TO_FILE

df = pd.read_csv(PATH_TO_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'])

# Najczęściej zgłaszane skargi
start_time_query_1 = time.perf_counter()
# query_1 = df['Complaint Type'].value_counts().head(1)
end_time_query_1 = time.perf_counter()

print('-------najczęściej zgłaszane skargi-------')
# print(query_1)
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
print('\nCzas: ', end_time_query_1 - start_time_query_1)
print('-------------------------------------------------------------')
