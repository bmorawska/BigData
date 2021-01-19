import re
import sys
import itertools as it
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

dir = "./Data/"

def PlotPivot(data, columns, title):
    pivot = data.pivot(columns[0],columns[1],columns[2])
    ax = sns.heatmap(pivot)
    #plt.title(title)
    #plt.show()
    plt.savefig("./Fig/"+title)
    plt.close()


#1.	Procent odwołanych, opoźnonych i przekierowanych
names = ['year', 'month', 'day', 'flights']
df_all = pd.read_csv("./Data/mostPopular.csv", header=0, names=names)
df_all_sum = df_all['flights'].sum()

#a Przekierowane (divorted)
names = ['year', 'month', 'day', 'divorted']
df_d = pd.read_csv("./Data/divertedFlights.csv", header=0, names=names)
df_d_percent = (df_d['divorted'].sum() / df_all_sum) * 100

#b Odwołane (Cancelled)
names = ['year', 'month', 'day', 'cancelled']
df_c = pd.read_csv("./Data/cancelledFlights.csv", header=0, names=names)
df_c_percent = (df_c['cancelled'].sum() / df_all_sum) * 100

#c Opóźnione (Delayed >0)
names = ['year', 'month', 'delayed']
df_del = pd.read_csv("./Data/mergedACnt_del.csv", header=0, names=names)
df_del_percent = (df_del['delayed'].sum() / df_all_sum) * 100

#d Opóźnione (Delayed >5)
names = ['year', 'month', 'delayed']
df_del_5 = pd.read_csv("./Data/mergedBCnt_del.csv", header=0, names=names)
df_del_percent_5 = (df_del_5['delayed'].sum() / df_all_sum) * 100

#e Planowe (Delayed <=0)
onTime = df_all_sum - (df_d['divorted'].sum() + df_c['cancelled'].sum() + df_del['delayed'].sum())
onTime_percent = (onTime/df_all_sum) * 100

print("----------")
print("df_all_sum ", df_all_sum)
print("df_d_percent ", df_d_percent)
print("df_c_percent ", df_c_percent)
print("df_del_percent ", df_del_percent)
print("df_del_percent_5 ", df_del_percent_5)
print("onTime_percent ", onTime_percent)

def plotPie(data, title):
    labels =  'Divorted', 'Cancelled','Delayed', 'OnTime'
    explode = (0.1, 0, 0, 0.0)
    fig1, ax1 = plt.subplots()
    ax1.pie(data, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
    #plt.title(title)
    #plt.show()
    plt.savefig(title)
    plt.close()


data = []
data.append(df_d_percent)
data.append(df_c_percent)
data.append(df_del_percent)
data.append(onTime_percent)

plotPie(data, "./Fig/1_a_Pie.jpg")

data[2] = df_del_percent_5

plotPie(data, "./Fig/1_b_Pie.jpg")
#KONIEC 1.

#2.	Opóźnienie heat mapa 

## HEAT MAP DELAYED 
names = ['year', 'month', 'delayed']
df_del_ym = pd.read_csv(dir + "yearly_del.csv", header=0, names=names)
df_del_ym['delayed'] =df_del_ym['delayed']                               #SCALA!
names = ['day_of_week', 'hour', 'delayed']
df_del_dh = pd.read_csv(dir + "hourly_del.csv", header=0, names=names).dropna()
names = ['year', 'month', 'day_of_week', 'delayed']

df_del_md_t = pd.read_csv(dir + "delFlights.csv", header=0, names=names, usecols=['month', 'day_of_week', 'delayed']).dropna()
df_del_md_t.groupby(['month', 'day_of_week']).sum().to_csv("./Delayed_Month_Day.csv")
names = ['month', 'day_of_week', 'delayed']
df_del_md = pd.read_csv("./Delayed_Month_Day.csv", header=0, names=names).dropna()

# YEAR-MONTH - DELAYED -> plot
PlotPivot(df_del_ym, ['year', 'month', 'delayed'], "2_Delayed_Year_Month.jpg")
# DAY-HOUR - DELAYED -> plot
PlotPivot(df_del_dh, ['day_of_week', 'hour', 'delayed'], "2_Delayed_Day_hour.jpg")
# MONTH - DAY
PlotPivot(df_del_md, ['month', 'day_of_week', 'delayed'], "2_Delayed_month_day.jpg")
## HEAT MAP DELAYED END
#2. KONIEC


#3.	Przekierowane heat mapa 
## HEAT MAP DIVERTED 
names = ['year', 'month', 'day_of_week', 'diverted']
df = pd.read_csv(dir + "divertedFlights.csv", header=0, names=names)
df_d_ym = df[['year', 'month', 'diverted']]
df_d_md = df[['month', 'day_of_week', 'diverted']]
# YEAR-MONTH - DIVERTED
df_d_ym.groupby(['year', 'month']).sum().to_csv("./Diverted_Year_Month.csv")
# MONTH-DAY - DIVERTED
df_d_md.groupby(['month', 'day_of_week']).sum().to_csv("./Diverted_Month_Day.csv")

# YEAR-MONTH - DIVERTED -> plot
df = pd.read_csv( "./Diverted_Year_Month.csv", header=0)
PlotPivot(df, ['year', 'month', 'diverted'], "3_Diverted_Year_Month.jpg")

# MONTH-DAY - DIVERTED -> plot
df = pd.read_csv( "./Diverted_Month_Day.csv", header=0)
#PlotPivot(df, [ 'day_of_week', 'month','diverted'], "3_Diverted_Day_Month.jpg")
PlotPivot(df, [  'month','day_of_week','diverted'], "3_Diverted_Month_Day.jpg")
## HEAT MAP DIVERTED END
#3. Koniec

#4.	Odwołanie heat mapa
## HEAT MAP CANCELLED
names = ['year', 'month', 'day_of_week', 'cancelled']
df = pd.read_csv(dir + "cancelledFlights.csv", header=0, names=names)
df_c_ym = df[['year', 'month', 'cancelled']]
df_c_md = df[['month', 'day_of_week', 'cancelled']]

# YEAR-MONTH - CANCELLED
df_c_ym.groupby(['year', 'month']).sum().to_csv("./Cancelled_Year_Month.csv")
# MONTH-DAY - CANCELLED
df_c_md.groupby(['month', 'day_of_week']).sum().to_csv("./Cancelled_Month_Day.csv")

# YEAR-MONTH - CANCELLED -> plot
df = pd.read_csv( "./Cancelled_Year_Month.csv", header=0)
PlotPivot(df, ['year', 'month', 'cancelled'], "4_Cancelled_Year_Month.jpg")

# MONTH-DAY - CANCELLED -> plot
df = pd.read_csv( "./Cancelled_Month_Day.csv", header=0)
#PlotPivot(df, [ 'day_of_week', 'month','cancelled'], "4_Cancelled_Day_Month.jpg")
PlotPivot(df, [  'month','day_of_week','cancelled'], "4_Cancelled_Month_Day.jpg")
## HEAT MAP CANCELLED END
#4. KONIEC

#5.	Typy opóźnień -  liczba minut opóźnień dla każdego rodzaju opóźnienia
## DELAY TYPE PER MONTH
names=['month', 'CarrierDelay', 'WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay']
df_del_type = pd.read_csv("./Data/delay_type.csv", header=0, names=names)

x_label = df_del_type['month']
y_label_CarrierDelay = df_del_type['CarrierDelay']
y_label_WeatherDelay = df_del_type['WeatherDelay']
y_label_NASDelay = df_del_type['NASDelay']
y_label_SecurityDelay = df_del_type['SecurityDelay']
y_label_LateAircraftDelay = df_del_type['LateAircraftDelay']

plt.plot(y_label_CarrierDelay, color='green')
plt.plot(y_label_WeatherDelay, color='red')
plt.plot(y_label_NASDelay, color='orange')
plt.plot(y_label_SecurityDelay, color='purple')
plt.plot(y_label_LateAircraftDelay, color='yellow')
plt.legend(['CarrierDelay', 'WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'])
plt.xlabel("Month")
plt.ylabel("Delay [min]")
plt.show()
plt.savefig("./Fig/5_delay_type.jpg")
plt.close()
#5. KONIEC

#6.	Opóźnienie samolotów
#a.	Ilość opóźnionych lotów dla każdego rocznika samolotu 
names=['age', 'delay']
df_a_del = pd.read_csv("./Data/cnt_delayFlight_plane.csv", header=0, names=names)
df_a_del['age'] = df_a_del[ df_a_del['age']>=0 ] 
df_a_del['age'] = df_a_del[ df_a_del['age']<200 ] 
plt.plot(df_a_del['age'], df_a_del['delay']/1000000)
plt.xlabel("Age of aircraft")
plt.ylabel("Delay [mln min]")
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/6_a_cnt_delayFlight_plane_6a.jpg")
plt.close()

#b.	Procent opóźnionych lotów dla każdego rocznika 
names=['age', 'delay']
df_a_del_all = pd.read_csv("./Data/delayFlight_planeCntAll.csv", header=0, names=names)
df_a_del_all['age']= df_a_del_all[ df_a_del_all['age']>=0 ] 
df_a_del_all['age'] = df_a_del_all[ df_a_del_all['age']<200 ] 

df_a_del_all['factor'] =  (df_a_del['delay'] / df_a_del_all['delay']) *100

plt.plot(df_a_del['age'], df_a_del_all['factor'])
plt.xlabel("Age of aircraft")
plt.ylabel("The percentage of delayed flights")
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/6_b_cnt_delayFlight_plane_6b.jpg")
plt.close()

#7.	Najpopularniejsze linie lotnicze   
names=['airline', 'flights']
df_a_pop = pd.read_csv("./Data/allFlightsAirlineCNT.csv", header=0, names=names)

names=['airline', 'airlineFullName']
df_carrieres = pd.read_csv("./Data/carriers.csv", header=0, names=names)
df_a_pop = df_a_pop.merge(df_carrieres, how='inner', on='airline')
df_a_pop.sort_values(by=['flights'], inplace=True, ignore_index=True, ascending=False)

print(df_a_pop)

plt.bar(df_a_pop['airlineFullName'], df_a_pop['flights']/1000000)
plt.xticks(rotation=90)
plt.xlabel("Airline")
plt.ylabel("Number of flights [mln]")
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/7_allFlightsAirlineCNT_7.jpg")
plt.close()

#7. KONIEC

#8.	Średni czas opóźnienia przylotu(arrival) i odlotu(dep) dla linii lotniczej
names=['airline', 'delay']
df_a_del_arr_sum = pd.read_csv("./Data/delayAirlineArr.csv", header=0, names=names)
df_a_del_arr_sum.sort_values(by=['airline'], inplace=True, ignore_index=True)

names=['airline', 'delay']
df_a_del_arr_cnt = pd.read_csv("./Data/AirlinesFlightsDelay2.csv", header=0, names=names)
df_a_del_arr_cnt.sort_values(by=['airline'], inplace=True, ignore_index=True)

df_a_del_arr_cnt['mean'] = (df_a_del_arr_sum['delay']/ df_a_del_arr_cnt['delay']) 

df_a_del_arr_cnt = df_a_del_arr_cnt.merge(df_carrieres, how='inner', on='airline')

"""
plt.bar(df_a_del_arr_cnt['airlineFullName'], df_a_del_arr_cnt['mean'])
plt.xticks(rotation=90)
plt.show()
"""

names=['airline', 'delay']
df_a_del_dep_sum = pd.read_csv("./Data/delayAirlineDep.csv", header=0, names=names)
df_a_del_dep_sum = df_a_del_dep_sum.merge(df_carrieres, how='inner', on='airline')

df_a_del_dep_sum.sort_values(by=['airline'], inplace=True, ignore_index=True)
names=['airline', 'delay']
df_a_del_dep_cnt = pd.read_csv("./Data/delayAirlineDepCnt.csv", header=0, names=names)

df_a_del_dep_cnt = df_a_del_dep_cnt.merge(df_carrieres, how='inner', on='airline')

df_a_del_dep_cnt.sort_values(by=['airline'], inplace=True, ignore_index=True)
print(df_a_del_dep_sum)
df_a_del_dep_cnt['mean'] = (df_a_del_dep_sum['delay']/ df_a_del_dep_cnt['delay']) 

print("df_a_del_dep_cnt")
print(df_a_del_dep_cnt)

"""
plt.bar(df_a_del_dep_cnt['airline'], df_a_del_dep_cnt['mean'])
plt.xticks(rotation=90)
plt.show()
"""

#df_a_del_arr_cnt = df_a_del_arr_cnt.merge(df_carrieres, how='inner', on='airline')
#df_a_del_dep_cnt = df_a_del_dep_cnt.merge(df_carrieres, how='inner', on='airline')

print(df_a_del_arr_cnt)

width = 0.35 
fig, ax = plt.subplots()
ax.bar(df_a_del_arr_cnt['airlineFullName'], df_a_del_arr_cnt['mean'], 0.40,label='Arrival')
ax.bar(df_a_del_dep_cnt['airlineFullName'], df_a_del_dep_cnt['mean'], 0.30,label='Departure')

ax.set_ylabel('Mean delay [min]')
#ax.set_title('mean del arr/dep')
plt.xticks(rotation=90)
#ax.legend()
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/8_delayAirlineArrDep_8.jpg")
plt.close()

#8. KONIEC



#9.	Procent opóźnionych lotów dla każdej linii lotniczej
names=['airline', 'flights']
df_a_all_cnt = pd.read_csv("./Data/allFlightsAirlineCNT.csv", header=0, names=names)
df_a_all_cnt
df_a_all_cnt = df_a_all_cnt[df_a_all_cnt.airline != 'KH'].reset_index(drop=True)
names=['airline', 'flights']
df_a_del_all = pd.read_csv("./Data/AirlinesFlightsDelay2.csv", header=0, names=names)
df_a_del_all = df_a_del_all.merge(df_carrieres, how='inner', on='airline')

print("df_a_all_cnt")
print(df_a_all_cnt)
print("df_a_del_all")
print(df_a_del_all)

#df_a_del_all['mean'] = df_a_all_cnt['flights']/df_a_del_all['flights']
df_a_del_all['mean'] = (df_a_del_all['flights']/df_a_all_cnt['flights'])*100
print(df_a_del_all['mean'] )

df_a_del_all.sort_values(by=['mean'], inplace=True, ignore_index=True, ascending=False)
plt.bar(df_a_del_arr_cnt['airlineFullName'], df_a_del_all['mean'])
plt.xticks(rotation=90)
#plt.title("Mean Delay per airline")
plt.xlabel("Airline")
plt.ylabel("Mean delay [min]")
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/9_allFlightsAirlineCNT.jpg")
plt.close()

#9. KONIEC


#10.	Najpopularniejsze destynacje
names=['destination', 'flights']
fig,ax= plt.subplots()
df_pop_dest =  pd.read_csv("./Data/popularDest.csv", header=0, names=names)

df_pop_dest_max = df_pop_dest[:15]
df_pop_dest_max['flights'] = df_pop_dest_max['flights']/1000

df_pop_dest_min = df_pop_dest[-15:]
df_pop_dest_min['Flights'] = df_pop_dest_min['flights']/1000

df_pop_dest_max.plot(kind='bar',ax = ax, x='destination',y='flights',color='green')
ax.set_xlabel("Destination")
ax.set_ylabel("Number of flights(thousands)")
#ax.set_title(("The most popular destination"), fontsize=10)
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/10_1_popularDest.jpg")
plt.close()


fig,ax= plt.subplots()
df_pop_dest_min.plot(kind='bar',ax = ax, x='destination',y='flights',color='green')
ax.set_xlabel("Destination")
ax.set_ylabel("Number of flights")
#ax.set_title(("The least popular destination"), fontsize=10)
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/10_2_popularDest.jpg")
plt.close()

#10. KONIEC

#11.	Najbardziej opóźnione destynacje
names = ['destination', 'delay']
df_pop_dest_del = pd.read_csv(dir + "popularDestDel.csv", header=0, names=names)
df_pop_dest_del.sort_values(by=['delay'], inplace=True, ignore_index=True, ascending=False)

df_pop_dest_del = df_pop_dest_del[:15]


plt.bar(df_pop_dest_del['destination'], df_pop_dest_del['delay']/1000000)
#plt.title("The most delayed airport")
plt.ylabel("Delay [mln min]")
plt.xticks(rotation=90)
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/11_popularDestDel.jpg")
plt.close()

#11. KONIEC

#12.	Korelacja 
#names = ['Rok', 'Miesiąc', 'Dzień miesiąca', 'Dzień tygodnia', 'Czas przylotu', 'Opóźnienie odlotu', 'Opóźnienie przylotu', 'Czas w powietrzu', 'Dystans', 'Opóźnienie przewoźnika', 'Opóźnienie pogodowe', 'Opóźnienie NAS', 'Opóźnienie ws. bezpieczeństwa', 'Opóźnienie poprzedniego lotu']
names = [ 'Year','Month', 'Day of Month','Day of week', 'Departure Time', 'Departure delay (min)', 'Arrival delay (min)','AirTime','Distance', 'CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay']

data = pd.read_csv('./Data/CorrPearson.csv', sep=",", header=None, names = names)
ax = sns.heatmap(data,yticklabels=names,xticklabels=names)
#plt.title("Korelacja - Pearson")
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/12_CorrPearson.jpg")
plt.close()

data = pd.read_csv('./Data/CorrSpearman.csv', sep=",", header=None, names = names)
ax = sns.heatmap(data,yticklabels=names,xticklabels=names)
#plt.title("Korelacja - Pearson")
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/12_CorrSpearman.jpg")
plt.close()

#13.	Potok ludzi 
#a. Lotnisko docelowe
names = ['destination', 'passengers']
df_pop_dest_pass = pd.read_csv(dir + "mainDest.csv", header=0, names=names)
df_pop_dest_pass.sort_values(by=['passengers'], inplace=True, ignore_index=True, ascending=False)

names = ['destination', 'airport']
df_dest_desc = pd.read_csv(dir + "origin_dest_descript.csv", header=0, names=names)
df_pop_dest_pass = df_pop_dest_pass.merge(df_dest_desc, how='inner', on='destination')


df_pop_dest_pass_max = df_pop_dest_pass[:15]
df_pop_dest_pass_min = df_pop_dest_pass[-15:]
#df_pop_dest_pass_max['passengers'] = df_pop_dest_pass_max['passengers']#/1000000
print(df_pop_dest_pass_max)

#df_pop_dest_pass_min['passengers'] = df_pop_dest_pass_min['passengers']
print(df_pop_dest_pass_min)
x = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15] 
plt.bar(df_pop_dest_pass_max['airport'], df_pop_dest_pass_max['passengers']/1000000)
plt.xticks(rotation=90)
#plt.title("Lotniska z największą ilością pasażerów (mln)")
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/13_a_mainDest_max.jpg")
plt.close()


plt.bar(df_pop_dest_pass_min['airport'], df_pop_dest_pass_min['passengers'])
plt.xticks(rotation=90)
#plt.title("Lotniska z najmniejszą ilością pasażerów")
plt.tight_layout()
#plt.show() 
plt.savefig("./Fig/13_a_mainDest_min.jpg")
plt.close()




#b. Lotnisko wyjściowe
names = ['origin', 'passengers']
df_pop_origin_pass = pd.read_csv(dir + "mainOrigin.csv", header=0, names=names)
df_pop_origin_pass.sort_values(by=['passengers'], inplace=True, ignore_index=True, ascending=False)

names = ['origin', 'airport']
df_dest_desc = pd.read_csv(dir + "origin_dest_descript.csv", header=0, names=names)
df_pop_origin_pass = df_pop_origin_pass.merge(df_dest_desc, how='inner', on='origin')


df_pop_origin_pass_max = df_pop_origin_pass[:15]
df_pop_origin_pass_min = df_pop_origin_pass[-15:]
#df_pop_dest_pass_max['passengers'] = df_pop_dest_pass_max['passengers']#/1000000
print(df_pop_origin_pass_max)

#df_pop_dest_pass_min['passengers'] = df_pop_dest_pass_min['passengers']
print(df_pop_origin_pass_min)
x = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15] 
plt.bar(df_pop_origin_pass_max['airport'], df_pop_origin_pass_max['passengers']/1000000)
plt.xticks(rotation=90)
#plt.title("Lotniska z największą ilością pasażerów (mln)")
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/13_b_mainOrigin_min.jpg")
plt.close()


plt.bar(df_pop_origin_pass_min['airport'], df_pop_origin_pass_min['passengers'])
plt.xticks(rotation=90)
#plt.title("Lotniska z najmniejszą ilością pasażerów")
plt.tight_layout()
#plt.show()  
plt.savefig("./Fig/13_b_mainOrigin_min.jpg")
plt.close()



#c. Różnice
width = 0.35 
fig, ax = plt.subplots()
ax.bar(df_pop_origin_pass_max['airport'], df_pop_dest_pass_max['passengers']/1000000, 0.40,label='Dest')
ax.bar(df_pop_origin_pass_max['airport'], df_pop_origin_pass_max['passengers']/1000000, 0.30,label='Origin')

ax.set_ylabel('Passengers [mln]')
#ax.set_title('POrownianie')
plt.xticks(rotation=90)
#ax.legend()
plt.tight_layout()
#plt.show()
plt.savefig("./Fig/13_c_mainOrigin_Dest_max.jpg")
plt.close()



diff_max = df_pop_dest_pass_max['passengers'] - df_pop_origin_pass_max['passengers']
print(diff_max)
diff_min = df_pop_dest_pass_min['passengers'] - df_pop_origin_pass_min['passengers']
print(diff_min)


#d. Połączenia


names = ['origin', 'destination', 'passengers']
df_conn = pd.read_csv(dir + "mainOriginDest.csv", header=0, names=names)
df_conn.sort_values(by=['passengers'], inplace=True, ignore_index=True, ascending=False)

namesO = ['origin', 'airport']
namesD = ['destination', 'airport']
df_dest_desc = pd.read_csv(dir + "origin_dest_descript.csv", header=0, names=namesO)
df_conn = df_conn.merge(df_dest_desc, how='inner', on='origin')
df_dest_desc = pd.read_csv(dir + "origin_dest_descript.csv", header=0, names=namesD)
df_conn = df_conn.merge(df_dest_desc, how='inner', on='destination')

df_conn['conn'] = df_conn['airport_x'] +" - " + df_conn['airport_y'] 

#print(df_conn)
df_conn.sort_values(by=['passengers'], inplace=True, ignore_index=True, ascending=False)
df_conn_max = df_conn[:30]

plt.bar(df_conn_max['conn'], df_conn_max['passengers']/1000000)
plt.xticks(rotation=90)
#plt.title("POTOK")
plt.ylabel("Passengers [mln]")
plt.xlabel("Route")
plt.tight_layout()
#plt.show()  
plt.savefig("./Fig/13_d_mainOriginDest_route.jpg")
plt.close()


input("KONIEC")




"""
MID TERM
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
plt.tight_layout()
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
plt.tight_layout()
plt.show()

fig,ax= plt.subplots()
df2_min.plot(kind='bar',ax = ax, x='_1',y='Flights',color='green')
ax.set_xlabel(" Destination")
ax.set_ylabel("Number of flights")
ax.set_title(("The least popular destination"), fontsize=10)
plt.tight_layout()
plt.show()

# 3
# Liczba opoznionych lotow w latach 1996-2020

fig,ax= plt.subplots()
df3.plot(kind='line',ax = ax, x='_1',y='_2',color='green')
ax.set_xlabel("Year")
ax.set_ylabel("Number of flights (Thousands)")
ax.set_title(("Number of delayed flights 1996-2020"), fontsize=10)
#plt.show()
"""

