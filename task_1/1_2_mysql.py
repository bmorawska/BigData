import time
import pandas as pd
import mysql.connector
from task_1.path import PATH_TO_FILE, PATH_TO_EXAMPLE_FILE

df = pd.read_csv(PATH_TO_EXAMPLE_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'])

conn = mysql.connector.connect(db='randomDB', host='localhost', user='root', password='123', port=3306)

db_cursor = conn.cursor()

insert_data_query = "INSERT INTO SERVICE_REQUESTS(AGENCY_NAME, COMPLAINT_TYPE, BOROUGH) VALUES(%s, %s, %s)"

for chunk in pd.read_csv(PATH_TO_FILE, usecols=['Agency Name', 'Complaint Type', 'Borough'], chunksize=10000):

    data = list(zip(chunk['Agency Name'], chunk['Complaint Type'], chunk['Borough']))

    try:
        db_cursor.executemany(insert_data_query, data)
        conn.commit()
        print('success')

    except (mysql.connector.Error, mysql.connector.Warning) as e:
        conn.close()
        print(e)

conn.close()