import re
import sys
import itertools as it
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import time

print("Spark Configuration...")
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
spark = SparkSession(sc)
print("SparkContext initiated ")

now = time.time()
header = sc.textFile("./header.csv")
file_rdd = sc.textFile("./All/2018/*.csv")
#file_rdd = sc.textFile("./All/*/*.csv")
loaded = time.time() - now
print("LOADED ", loaded)

now = time.time()
h = header.first()
file_rdd = file_rdd.map(lambda x:x).filter(lambda row : row != h)   #delete 
print("header ", time.time() - now )

now = time.time()
rows = file_rdd.map(lambda line : line.replace('"',''))
print("replace ", time.time() - now )

now = time.time()
rows = rows.map(lambda line : line.split(","))
clean = rows.map(lambda row : row)
print("clean ", time.time() - now )



def saveToCSV(data, name):
    data.to_csv(name, index = False)

def RDDToDF(data):
    now = time.time()
    #sparkDF = data.map(lambda x: str(x)).map(lambda w: w.split(',')).toDF()
    sparkDF = data.toDF()
    print("sparkDF ", time.time() - now )
    #Spark DataFrame to Pandas DataFrame
    now = time.time()
    pdsDF = sparkDF.toPandas()
    print("pdsDF ", time.time() - now )
    return pdsDF

#saveToCSV( RDDToDF(AirlinesFlights), "./AirlinesFlightsCNT.csv" )




now = time.time()
# 0  -> year
# 2  -> month
# 6  -> airline
# 33 -> departure delay (min)
# 44 -> arrival delay (min)
# 49 -> cancelled (0/1)
# 51 -> diverted (0/1)

delayFlight = clean.map(lambda x: (x[0], x[2], x[33], x[44], x[49], x[51], x[6])).filter(lambda x : x[2] != "" ).filter(lambda x : x[3] != "" ).filter(lambda x : x[4] == '0.00').filter(lambda x : x[5] == "0.00")
print("notCancelled ", time.time() - now )
"""
now = time.time()
print("delayFlightFloat ", time.time() - now )



### Opoźnione przyloty per miesiac per lata
# 1) Suma czasu opóźnionych 
#       a) del ( >0 )
#       b) del ( >5 )

# 2) Liczba opóźnionych
#       a) del ( >0 )
#       b) del ( >5 )

# połączyć rok + miesiąc (x[0], x[1])
#       postać: ( (x[0], x[1])  , x[44] )
#               ( (1997, 1)     , 12.0 )
# filtr >0  albo >5
# dla liczby:
# map ((x[0],x[1]), 1) -> zliczenie 1
# dla sumy:
# proceura wiadoma 

p1 = delayFlight.map(lambda x: ( (x[0],x[1]), float(x[3]) )  )          # ((rok, miesiac), opoźnienie_przylotu)

p1f1 = p1.filter(lambda x: x[1]>0)
p1f2 = p1.filter(lambda x: x[1]>5)

sumA = p1f1.reduceByKey(lambda item1, item2: item1 + item2)
sumB = p1f2.reduceByKey(lambda item1, item2: item1 + item2)

#złączenie roku-miesiac
mergedA = sumA.map(lambda x: (x[0][0], x[0][1], x[1])  )
mergedB = sumB.map(lambda x: (x[0][0], x[0][1], x[1])  )
#merged = sumA.map(lambda x: (x[0][0] + x[0][1] , x[1]) )

now = time.time()
saveToCSV(RDDToDF(mergedA),"./sumA.csv")
print("sumA ", time.time() - now )

now = time.time()
saveToCSV(RDDToDF(mergedB),"./sumB.csv")
print("sumB ", time.time() - now )


#SUMA CNT
p1f1One = p1f1.map(lambda x: ( x[0], 1) )
p1f2One = p1f2.map(lambda x: ( x[0], 1) )

sumA2 = p1f1One.reduceByKey(lambda item1, item2: item1 + item2)
sumB2 = p1f2One.reduceByKey(lambda item1, item2: item1 + item2)
#złączenie roku-miesiac
mergedA2 = sumA2.map(lambda x: (x[0][0], x[0][1], x[1])  )
mergedB2 = sumB2.map(lambda x: (x[0][0], x[0][1], x[1])  )

now = time.time()
saveToCSV(RDDToDF(mergedA2),"./sumA_CNT.csv")
print("sumA_CNT ", time.time() - now )

now = time.time()
saveToCSV(RDDToDF(mergedB2),"./sumB_CNT.csv")
print("sumB_CNT ", time.time() - now )




# Ilosc lotów w ciagu miesiąca kazdego roku
# 1) Z clean bierzemy wszytskie elementy - tworzymy date z 1: ((x[0] ,x[1]) , 1)
# 2) Zliczamy 1 i zapisujemy

AllFlights = clean.map(lambda x: ((x[0], x[2]), 1))
AllFlightsPerMonthAndYear = AllFlights.reduceByKey(lambda item1, item2 :  item1 + item2)
mergedAllFlights = AllFlightsPerMonthAndYear.map(lambda x: (x[0][0], x[0][1], x[1])  )

now = time.time()
saveToCSV( RDDToDF(mergedAllFlights), "./AllFlights.csv" )
print("AllFlights ", time.time() - now )




# Najpopularniejsze destynacje
# 1) Pobranie danych. Zapis w postaci (miasto, 1)
#       25 -> Destination City Name

# 2) Zliczenie wystąpień.

City = clean.map(lambda x : (x[25], 1))
popularDest = City.reduceByKey(lambda item1, item2: item1 + item2)
sortedPopDest = popularDest.sortBy(lambda a: -a[1])

now = time.time()
saveToCSV( RDDToDF(sortedPopDest), "./popularDest.csv" )
print("AllFlights ", time.time() - now )
"""

# procent opoznionych lotow przez dana linie.
# Delay flight. Count of delay flight by airlines. Fliter ArrDelay > 1 (opozniony) + gruoupby(AIline, 1) + sum

notCancelled = delayFlight.map(lambda x: (x[6], float(x[3]) ))
print(notCancelled.take(2))
a1f1 = notCancelled.filter(lambda x: x[1]>0)
a1f2 = notCancelled.filter(lambda x: x[1]>5)

a1f1ToOne = a1f1.map(lambda x: (x[0], 1))
a1f2ToOne = a1f2.map(lambda x: (x[0], 1))
print(a1f2ToOne.take(1))
airlineDelay1 = a1f1ToOne.reduceByKey(lambda item1, item2 :  item1 + item2)
airlineDelay2 = a1f2ToOne.reduceByKey(lambda item1, item2 :  item1 + item2)

now = time.time()
saveToCSV( RDDToDF(airlineDelay1), "./AirlinesFlightsDelay1.csv" )
print("airlineDelay1 ", time.time() - now )

now = time.time()
saveToCSV( RDDToDF(airlineDelay2), "./AirlinesFlightsDelay2.csv" )
print("airlineDelay2 ", time.time() - now )

# ogolnie ile linie mialy lotow

allFlightsAirline = clean.map(lambda x: (x[6], 1 ))

allFlightsAirlineCNT = allFlightsAirline.reduceByKey(lambda item1, item2 :  item1 + item2)

now = time.time()
saveToCSV( RDDToDF(allFlightsAirlineCNT), "./allFlightsAirlineCNT.csv" )
print("allFlightsAirlineCNT ", time.time() - now )







input("KONIEC ")

sc.stop()