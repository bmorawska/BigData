import time
import pandas as pd

from task_1.path import PATH_TO_FILE, PATH_TO_EXAMPLE_FILE

df = pd.read_csv(PATH_TO_FILE, low_memory=False)

# Najczęściej zgłaszane skargi
start_time_query = time.perf_counter()
query = df['Complaint Type'].value_counts().sort_values(ascending=False).head(5)
end_time_query = time.perf_counter()

print('-------najczęściej zgłaszane skargi-------')
print(query)
print('\nCzas: ', end_time_query - start_time_query)
print('------------------------------------------')

