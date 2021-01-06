import re
import sys
import itertools as it
import pandas as pd

import matplotlib.pyplot as plt


# 1

fig,ax= plt.subplots()
df = pd.read_csv("./ALLWYNIKI/popularDest.csv")
df['_1'] = df['_1'].str.replace('\'', '')
df = df[:15]
df['Flights'] = df['_2']/1000
df.plot(kind='bar',ax = ax, x='_1',y='Flights',color='green')
ax.set_xlabel("Destination")
ax.set_ylabel("Number of flights(thousands)")
ax.set_title(("The most popular destination"), fontsize=10)
#plt.show()

#2


fig,ax= plt.subplots()

df1 = pd.read_csv("./ALLWYNIKI/AirlinesFlightsDelay.csv")
df2 = pd.read_csv("./ALLWYNIKI/AirlinesFlightsCNT.csv")


df2.sort_values(by=['_2'], inplace=True, ascending=False)
df2['_2'] = df2['_2']/100000
df2.plot(kind='bar' , ax=ax,x='_1',y='_2',color='green')
ax.set_xlabel("Airline")
ax.set_ylabel("Flights (Milions)")
ax.set_title(("Number of flights per Airline"), fontsize=10)
#plt.show()


fig,ax= plt.subplots()

df0 = pd.merge(df1, df2, on='_1')

print(df0)
df0['Delay%'] = (df0['_2_x'] / df0['_2_y'])*100
df0.sort_values(by=['Delay%'], inplace=True, ascending=False)

print(df0)

#sum delat all
allDelay =  df0['_2_x'].sum()
#sum  all
all =  df0['_2_y'].sum()

Avg = (allDelay/all)*100
print(Avg)

df0.plot(kind='bar' , ax=ax,x='_1',y='Delay%',color='green')
ax.set_xlabel("Airline")
ax.set_ylabel("% delayed lights")
ax.set_title(("Percentage of delayed flights per Airline"), fontsize=10)
ax.axhline(Avg)
#plt.show()



#3

df3 = pd.read_csv("./ALLWYNIKI/DelayCntPerYear.csv")
df3.sort_values(by=['_1'], inplace=True, ascending=False)
print(df3)
df3['_2'] = df3['_2']/1000
fig,ax= plt.subplots()
df3.plot(kind='line',ax = ax, x='_1',y='_2',color='green')
ax.set_xlabel("Year")
ax.set_ylabel("Number of flights (Thousands)")
ax.set_title(("Number of delayed flights 1996-2020"), fontsize=10)
#plt.show()

#4

df4 = pd.read_csv("./ALLWYNIKI/sumOfDelayAll.csv")
df4.sort_values(by=['_1'], inplace=True, ascending=True)
print(df4)
fig,ax= plt.subplots()
df4.plot(kind='bar',ax = ax, x='_1',y='_2',color='green')
ax.set_xlabel("Year")
ax.set_ylabel("Minutes")
ax.set_title(("Sum of delayed minutes 1996-2020"), fontsize=10)
plt.show()


# godziny
df4['_2'] = df4['_2']/60000

print(df4)
fig,ax= plt.subplots()
df4.plot(kind='bar',ax = ax, x='_1',y='_2',color='green')
ax.set_xlabel("Year")
ax.set_ylabel("Thousand hours")
ax.set_title(("Sum of delayed hours 1996-2020"), fontsize=10)
plt.show()
