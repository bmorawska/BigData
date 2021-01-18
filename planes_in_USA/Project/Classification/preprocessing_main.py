import numpy as np
import pandas as pd

from planes_in_USA.Project.Classification.classifiers import do_classification
from planes_in_USA.Project.Classification.graphs import *

pd.set_option('display.max_columns', None)

headers = ["Year", "Quarter", "Month", "DayofMonth", "DayOfWeek", "Reporting_Airline", "Origin", "Dest",
           "CRSDepTime",
           "DepTime", "DepDelay", "TaxiOut", "WheelsOff", "WheelsOn", "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay",
           "Cancelled",
           "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance"]

dtypes = {"Year": "int16", "Quarter": "int8", "Month": "int8", "DayofMonth": "int8", "DayOfWeek": "int8",
          "Reporting_Airline": "category", "Origin": "category", "Dest": "category", "CRSDepTime": "int16",
          "DepTime": "float32",
          "DepDelay": "float32", "TaxiOut": "float32", "WheelsOff": "float32", "WheelsOn": "float32",
          "TaxiIn": "float32", "CRSArrTime": "int16",
          "ArrTime": "float32", "ArrDelay": "float32", "Cancelled": "float32", "Diverted": "float32",
          "CRSElapsedTime": "float32", "ActualElapsedTime": "float32",
          "AirTime": "float32", "Distance": "float32"}



df = pd.read_csv("planes_in_USA/data/2020/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2020_3.csv", usecols=headers, dtype=dtypes)
print(df.info())

## Czyszczenie danych z lotów anulowanych (których nie bierzemy pod uwagę, bo się nie odbyły)
print(df.Cancelled.value_counts())
df = df[(df['Cancelled'] == 0)]
df = df.drop(['Cancelled'], axis=1)
print(df.info())

## Sprawdzenie, jak wyglądają dane dla lotów przekierowanych (Diverted)
print(df.Diverted.value_counts())
print(df[df['Diverted'] == 1.0])

## Nieznany ArrDelay, porzucamy przekierowane samoloty
df = df[(df['Diverted'] == 0.0)]
df = df.drop(['Diverted'], axis=1)
print(df.info())

## Czyszczenie danych z NaN
print(df.isna().sum())  ## Jedynie 5 brakujących danych, można pominąć te wiersze
df = df.dropna()
print(df.info())  ## Brakujące dane były tylko w 1 wierszu

## Przeskalowanie kolumn
df['CRSDepTime'] = np.ceil(df['CRSDepTime']/100).apply("int8")
df['DepTime'] = np.ceil(df['DepTime']/100).apply("int8")
df['WheelsOff'] = np.ceil(df['WheelsOff']/100).apply("int8")
df['WheelsOn'] = np.ceil(df['WheelsOn']/100).apply("int8")
df['CRSArrTime'] = np.ceil(df['CRSArrTime']/100).apply("int8")
df['ArrTime'] = np.ceil(df['ArrTime']/100).apply("int8")

## Dodatkowa kolumna ze statusem do klasyfikacji
bools = []
for delay in df['ArrDelay']:
    if delay < 0:
        bools.append(0)
    else:
        bools.append(1)
df['FlightDelay'] = bools
df = df.drop(['ArrDelay'], axis=1)

print(df.head(3).append(df.tail(3)))

## Filtrowanie miast docelowych na najpopularniejsze (ponieważ potem będzie dokonywana operacja hot-coding na miastach)
cities = df.Dest.value_counts().iloc[0:30].rename_axis('Dest').reset_index(name='Counter')
print(cities.Dest.unique())
city_list = cities['Dest'].tolist()
bools_2 = df.Dest.isin(city_list)
df = df[bools_2]

df = df.drop(['Origin'], axis=1)

## Ponowne przeliczenie indeksu oraz aktualizowanie kategorii
df.reset_index(drop=True)
df.Dest = pd.Categorical(df['Dest'], categories=city_list)

print("\n\nDane po przefiltrowaniu miast docelowych")
print(df.info())
print(df.Dest.unique())

## check_distribution_classes(df)
## check_histograms(df)
## check_histograms_cat(df)

## Przekonwertowanie danych typu category/string na inty/floaty (transformacja koszykowa)
airline_dummies = pd.get_dummies(df['Reporting_Airline'], prefix='Airline', drop_first=True, dtype="int8")
dest_dummies = pd.get_dummies(df['Dest'], prefix='Dest', drop_first=True, dtype="int8")
CRSDepTime_dummies = pd.get_dummies(df['CRSDepTime'], prefix='CRSDepTime', drop_first=True, dtype="int8")
CRSArrTime_dummies = pd.get_dummies(df['CRSArrTime'], prefix='CRSArrTime', drop_first=True, dtype="int8")
Month_dummies = pd.get_dummies(df['Month'], prefix='Month', drop_first=True, dtype="int8")
DayOfWeek_dummies = pd.get_dummies(df['DayOfWeek'], prefix='DayOfWeek', drop_first=True, dtype="int8")

df = df.drop(['Reporting_Airline', 'Dest', 'CRSDepTime', 'CRSArrTime', 'Month', 'DayOfWeek'], axis=1)
df = pd.concat([df, airline_dummies, dest_dummies, CRSDepTime_dummies, CRSArrTime_dummies, Month_dummies,
                DayOfWeek_dummies], axis=1)

print(df.head(3).append(df.tail(3)))

print(df.info())

do_classification(df, True)
