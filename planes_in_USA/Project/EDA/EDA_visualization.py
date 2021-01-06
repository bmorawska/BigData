import re
import sys
import itertools as it
import pandas as pd

import matplotlib.pyplot as plt

dir = "./N/"

#### 1 ####
# Liczba lotów dla każdej linii lotniczej

df1 = pd.read_csv(dir + "allFlightsAirlineCNT.csv")
fig,ax= plt.subplots()

df1.sort_values(by=['_2'], inplace=True, ascending=False)
df1['_2'] = df1['_2']/100000
df1.plot(kind='bar' , ax=ax,x='_1',y='_2',color='green')
ax.set_xlabel("Airline")
ax.set_ylabel("Flights (Milions)")
ax.set_title(("Number of flights per Airline"), fontsize=10)
plt.show()

#### 2 ####
# Najpopularniejsze destynacje 
fig,ax= plt.subplots()
df2 =  pd.read_csv(dir + "popularDest.csv")
df2_max = df2[:15]
df2_max['Flights'] = df2_max['_2']/1000
df2_min = df2[-15:]
df2_min['Flights'] = df2_min['_2']

df2_max.plot(kind='bar',ax = ax, x='_1',y='Flights',color='green')
ax.set_xlabel("Destination")
ax.set_ylabel("Number of flights(thousands)")
ax.set_title(("The most popular destination"), fontsize=10)
plt.show()

fig,ax= plt.subplots()
df2_min.plot(kind='bar',ax = ax, x='_1',y='Flights',color='green')
ax.set_xlabel(" Destination")
ax.set_ylabel("Number of flights")
ax.set_title(("The least popular destination"), fontsize=10)
plt.show()

# 3
# Liczba opoznionych lotow w latach 1996-2020

fig,ax= plt.subplots()
df3.plot(kind='line',ax = ax, x='_1',y='_2',color='green')
ax.set_xlabel("Year")
ax.set_ylabel("Number of flights (Thousands)")
ax.set_title(("Number of delayed flights 1996-2020"), fontsize=10)
#plt.show()



# 4
# średnia ilosc opoznionych lotów wraz ze srednia dla linii


# 5
# Suma minut opóźnionych lotów


