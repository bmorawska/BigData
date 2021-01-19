import re
import sys
import itertools as it
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
import time
from pyspark.mllib.stat import Statistics

from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel


print("Spark Configuration...")
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
spark = SparkSession(sc)
print("SparkContext initiated ")

now = time.time()
header = sc.textFile("./header.csv")
file_rdd = sc.textFile("./All/*/*.csv")

h = header.first()
file_rdd = file_rdd.map(lambda x:x).filter(lambda row : row != h)   #delete 

now = time.time()
rows = file_rdd.map(lambda line : line.replace('"',''))
rows = rows.map(lambda line : line.split(","))

clean = rows.map(lambda row : row)

header_plane = sc.textFile("./plane/header.csv")
file_rdd_plane = sc.textFile("./plane/plane-data.csv")
h_plane = header_plane.first()


# tailNumber, year - bez pustych i "None"
file_rdd_planes = file_rdd_plane.map(lambda x:x).filter(lambda row : row != h_plane).map(lambda line : line.split(",")).map(lambda x : (x[0],x[8])).filter(lambda x: x[1] != "").filter(lambda x: x[1] != "None")

#print(file_rdd_planes.take(3))
file_rdd_planes = file_rdd_planes.map(lambda x : (x[0], int(x[1])))
#print(file_rdd_planes.take(10))

def saveToCSV(data, name):
    data.to_csv(name, index = False)

def RDDToDF(data):
    now = time.time()
    sparkDF = data.toDF()
    print("sparkDF ", time.time() - now )
    #Spark DataFrame to Pandas DataFrame
    now = time.time()
    pdsDF = sparkDF.toPandas()
    print("pdsDF ", time.time() - now )
    return pdsDF

#saveToCSV( RDDToDF(AirlinesFlights), "./AirlinesFlightsCNT.csv" )


# airportId, Airport city
dest = clean.map(lambda x: (x[11], x[15]) ).distinct()
saveToCSV( RDDToDF(dest), "./origin_dest_descript.csv" )


# Samoloty
#(model, tail)
aircraft = file_rdd_plane.map(lambda x:x).filter(lambda row : row != h_plane).map(lambda line : line.split(","))\
                            .map(lambda x : (x[4], x[0]))\
                            .filter(lambda x: x[0] !="")
#(model, capacity)
file_rdd_model = sc.textFile("./Data/13/model.csv").map(lambda x:x).map(lambda line : line.split(",")).map(lambda x: (x[0], x[1]))

#(model, (tail, capacity)) => #(tail, capacity)
aircraft_m = aircraft.join(file_rdd_model).map(lambda x: x[1])

#((year, month), factor)
factor = sc.textFile("./Data/13/load_factor.csv").map(lambda x:x).map(lambda line : line.split(",")).map(lambda x: ((float(x[0]), float(x[1])), float(x[2])/100  ))
#print(factor.take(10))

#((year, month), deest, tail)
#((year, month), tail, deest)
start = clean.map(lambda x: ((float(x[0]), float(x[2])),  (x[9], x[21])))
#print(start.take(100))

#((year, month), ((tail, dest), factor))        => (tail, factor, dest)
#((2020.0, 6.0), (('N474AS', '14747'), 56.16))
main = start.join(factor).map(lambda x: (x[1][0][0], (x[1][1], x[1][0][1])))
#print(main.take(3))

#('N760US', ((56.16, '13495'), '107'))
main2 = main.join(aircraft_m).map(lambda x: (x[1][0][1], float(x[1][1])* x[1][0][0])  ).reduceByKey(lambda item1, item2: item1+item2)
#print(main2.take(100))
saveToCSV( RDDToDF(main2), "./mainDest.csv" )



#((year, month), deest, tail, origin)
startO = clean.map(lambda x: ((float(x[0]), float(x[2])),  (x[9], x[11])))
#print(start.take(100))

#((year, month), ((tail, dest), factor))        => (tail, factor, dest)
#((2020.0, 6.0), (('N474AS', '14747'), 56.16))
mainO = startO.join(factor).map(lambda x: (x[1][0][0], (x[1][1], x[1][0][1])))
#print(main.take(3))


#('N760US', ((56.16, '13495'), '107'))
main2O = mainO.join(aircraft_m).map(lambda x: (x[1][0][1], float(x[1][1])* x[1][0][0])  ).reduceByKey(lambda item1, item2: item1+item2)
#print(main2.take(100))
saveToCSV( RDDToDF(main2O), "./mainOrigin.csv" )


#((year, month), tail, origin, deest)
startOD = clean.map(lambda x: ((float(x[0]), float(x[2])),  (x[9], x[11], x[21])))
#print(startOD.take(10))

#((year, month), ((tail, dest), factor))                => (tail, factor, dest)
#((year, month), ((tail, origin, dest), factor))        => (tail, factor, origin,dest)
#((2020.0, 6.0), (('N474AS', '14747'), 56.16))
mainOD = startOD.join(factor).map(lambda x: (x[1][0][0], (x[1][1], x[1][0][1], x[1][0][2])))
#print(mainOD.take(10))


#('N760US', ((56.16, '13495'), '107'))
main2OD = mainOD.join(aircraft_m).map(lambda x: ( (x[1][0][1], x[1][0][2]), float(x[1][1])* x[1][0][0])  ).reduceByKey(lambda item1, item2: item1+item2)
#print(main2OD.take(100))

saveToCSV( RDDToDF(main2OD), "./mainOriginDest.csv" )

# 0     0  -> year
# 1     2  -> month
# 2     33 -> departure delay (min)
# 3     44 -> arrival delay (min)
# 4     49 -> cancelled (0/1)
# 5     51 -> diverted (0/1)
# 6     6  -> airline
# 7     4  -> day of week
# 8     31 -> flight time (715 => 7:15)
# 9     58 -> CarrierDelay
# 10    59 -> WeatherDelay
# 11    60 -> NASDelay
# 12    61 -> SecurityDelay
# 13    62 -> LateAircraftDelay
# 14    46 -> ArrDel15
# 15    9  -> TailNumber
# 16    56 -> Distance
# 17    3 -> DayofMonth
# 18    32 -> DepTime
# 19    43 -> ArrTime
# 20    54 -> AirTime
#delayFlight = clean.map(lambda x: (x[0], x[2], x[33], x[44], x[49], x[51], x[6], x[4], x[31],  x[58],  x[59],  x[60],  x[61], x[62], x[46], x[9], x[56], x[3] )).filter(lambda x : x[2] != "" ).filter(lambda x : x[3] != "" ).filter(lambda x : x[4] == '0.00').filter(lambda x : x[5] == "0.00")

delayFlight = clean.map(lambda x: (x[0], x[2], x[33], x[44], x[49], x[51], x[6], x[4], x[31],  x[58],  x[59],  x[60],  x[61], x[62], x[46], x[9], x[56], x[3], x[32], x[43], x[54]))\
                        .filter(lambda x : x[2] != "" )\
                        .filter(lambda x : x[3] != "" )\
                        .filter(lambda x : x[4] == '0.00')\
                        .filter(lambda x : x[5] == "0.00")
                        
                        #.persist(StorageLevel.MEMORY_AND_DISK)

#print("ILOSC przed", delayFlight.count())

#Corr
corrTest = delayFlight.map(lambda x : (x[0], x[1],x[17] ,x[7],x[18],x[2] ,x[3],x[20] ,x[16],x[9] ,x[10],x[11],x[12],x[13] ))\
                        .filter(lambda x: x[0] != "")\
                        .filter(lambda x: x[1] != "")\
                        .filter(lambda x: x[2] != "")\
                        .filter(lambda x: x[3] != "")\
                        .filter(lambda x: x[9] != "")\
                        .map(lambda line: [float(i) for i in line])
                        

#print(corrTest.take(10))

#firstCol = corrTest.map(lambda x: x[0])
#secondCol = corrTest.map(lambda x: x[1])

#print("ILOSC firstCol", firstCol.count())
#print("ILOSC secondCol", secondCol.count())

#print("Correlation is: " + str(Statistics.corr(firstCol, secondCol, method="pearson")))
#print("Correlation is: " + str(Statistics.corr(corrTest, method="pearson")))
###CORR
cor1 = str(Statistics.corr(corrTest, method="pearson"))
text_file = open("CorrPearson.txt", "w")
text_file.write(cor1)
text_file.close()


#print("Correlation is: " + str(Statistics.corr(corrTest, method="spearman")))
cor2 = str(Statistics.corr(corrTest, method="spearman"))
text_file = open("./CorrSpearman.txt", "w")
text_file.write(cor2)
text_file.close()

# Podstawowe info

#liczba przekierowanych
divertedFlights = clean.map(lambda x: ( (x[0], x[2], x[4]), float(x[51]) )  ).reduceByKey(lambda item1, item2: item1+item2).map(lambda x: (x[0][0],x[0][1],x[0][2],x[1]))

print("divertedFlights")
#print(divertedFlights.take(10))
##saveToCSV( RDDToDF(divertedFlights), "./divertedFlights.csv" )

#liczba odwołanych
cancelledFlights = clean.map(lambda x: ( (x[0], x[2], x[4]), float(x[49]) )  ).reduceByKey(lambda item1, item2: item1+item2).map(lambda x: (x[0][0],x[0][1],x[0][2],x[1]))

print("cancelledFlights")
#print(cancelledFlights.take(10))
##saveToCSV( RDDToDF(cancelledFlights), "./cancelledFlights.csv" )


#liczba opóźnione
delFlights = clean.map(lambda x: ( (x[0], x[2], x[4]), x[44])).filter(lambda x: x[1] != "").filter(lambda x: float(x[1]) > 5).map(lambda x: ((x[0]), 1)).reduceByKey(lambda item1, item2: item1+item2).map(lambda x: (x[0][0],x[0][1],x[0][2],x[1]))

print("delFlights")
#print(delFlights.take(10))
saveToCSV( RDDToDF(delFlights), "./delFlights.csv" )


# linie lotnicze opoznione przyloty/ odloty
# delayFlight -> filtr: tylko opóźnione odloty ----> x[2] > 5 
delayAirlineDep = delayFlight.map(lambda x: (x[6], float(x[2]))).filter(lambda x: x[1] > 5).reduceByKey(lambda item1, item2: item1+item2)
#saveToCSV( RDDToDF(delayAirlineDep), "./delayAirlineDep.csv" )      #QWE

delayAirlineDepCnt = delayFlight.map(lambda x: (x[6], float(x[2]))).filter(lambda x: x[1] > 5).map(lambda x: (x[0], 1)).reduceByKey(lambda item1, item2: item1+item2)
##saveToCSV( RDDToDF(delayAirlineDepCnt), "./delayAirlineDepCnt.csv" )      #QWE


# Dni z największa ilością lotów
#mostPopular = delayFlight.map(lambda x : (( int(x[0]), int(x[1]), int(x[17])), 1) )#.map(lambda x : ((x[0], x[1], x[2]), 1))
#mostPopular = delayFlight.map(lambda x : (( x[0], x[1], x[17]), 1) )#.map(lambda x : ((x[0], x[1], x[2]), 1))

#print(mostPopular.take(60))

mostPopular = delayFlight.map(lambda x : (( int(x[0]), int(x[1]), int(x[17])), 1) )\
                            .reduceByKey(lambda item1, item2: item1+item2)\
                            .map(lambda x: (x[0][0],x[0][1], x[0][2], x[1] ))

#print(mostPopular.take(60))
###saveToCSV( RDDToDF(mostPopular), "./mostPopular.csv" )


### PLANS
delayFlightsForPlane =  delayFlight.map(lambda x : (x[15] ,(int(x[0]), float(x[3]))))
print("delayFlightsForPlane")

#delayFlight_planeNEW = delayFlightsForPlaneNEW.join(file_rdd_planes)
delayFlight_plane = delayFlightsForPlane.join(file_rdd_planes)

#print("delayFlight_plane")
#print(delayFlight_plane.take(50))
#print(delayFlight_planeNEW.take(3))

delayFlight_plane = delayFlight_plane.map(lambda x : (x[1][0][0] - x[1][1], x[1][0][1]))

#print(delayFlight_plane.take(50))

#(WIEK, OPOZNIENIE)
delayFlight_planeFiltered = delayFlight_plane.filter(lambda x : x[1] > 0.0)

#print(delayFlight_planeFiltered.take(10))

# Liczba lotów przepropwadzonych przez samolot z danego roku
delayFlight_planeCntAll = delayFlight_plane.map(lambda x: (x[0], 1)).reduceByKey(lambda item1, item2 : item1 + item2)  
    
#print(delayFlight_planeCntAll.take(10))
##saveToCSV( RDDToDF(delayFlight_planeCntAll), "./delayFlight_planeCntAll.csv" )  #QWE

#suma
sum_delayFlight_plane = delayFlight_planeFiltered.reduceByKey(lambda item1, item2 : item1 + item2)

#print(sum_delayFlight_plane.take(10))
#saveToCSV( RDDToDF(sum_delayFlight_plane), "./sum_delayFlight_plane.csv" )

#Ilsc
cnt_delayFlight_plane = delayFlight_planeFiltered.map(lambda x : (x[0], 1)).reduceByKey(lambda item1, item2 : item1 + item2)

#print(cnt_delayFlight_plane.take(10))
#saveToCSV( RDDToDF(cnt_delayFlight_plane), "./cnt_delayFlight_plane.csv" )

# hourly delay

def removeChar(tuple):
    string = tuple[1]
    Newstring = string[:-2]
    return ((tuple[0], Newstring),1)

#hourly_del = delayFlight.filter(lambda x: x[3] > 5).map(lambda x : ( x[7], x[8] )).map(removeChar).reduceByKey(lambda item1, item2 : item1 + item2)
hourly_del = delayFlight.map(lambda x: (x[7], x[8], float(x[3]) ))\
                        .filter(lambda x: x[2] > 5)\
                        .map(removeChar)\
                        .reduceByKey(lambda item1, item2 : item1 + item2)\
                        .map(lambda x: (x[0][0], x[0][1],x[1]))
                        
#print(hourly_del.collect())
#print(hourly_del.take(5))
##saveToCSV( RDDToDF(hourly_del), "./hourly_del.csv" )   #QWE

#yearly_del = delayFlight.filter(lambda x: x[3] > 5).map(lambda x : (( x[0], x[1] ),1 )).reduceByKey(lambda item1, item2 : item1 + item2)
yearly_del = delayFlight.map(lambda x : ( x[0], x[1], float(x[3])  ))\
                            .filter(lambda x: x[2] > 5)\
                            .map(lambda x : (( x[0], x[1] ),1 ))\
                            .reduceByKey(lambda item1, item2 : item1 + item2)\
                            .map(lambda x: (x[0][0], x[0][1],x[1]))
#print(yearly_del.take(5))
##saveToCSV( RDDToDF(yearly_del), "./yearly_del.csv" )   #QWE


# Rozaje opoznien

delay_type = delayFlight.filter(lambda x : x[14] == "1.00")\
                            .filter(lambda x : x[9] != "")\
                            .filter(lambda x : x[10] != "")\
                            .filter(lambda x : x[11] != "")\
                            .filter(lambda x : x[12] != "")\
                            .filter(lambda x : x[13] != "")\
                            .map(lambda x : ( x[1], ( float(x[9]), float(x[10]), float(x[11]), float(x[12]), float(x[13])) ))\
                            .reduceByKey(lambda item1, item2 : (item1[0] + item2[0], item1[1] + item2[1], item1[2] + item2[2], item1[3] + item2[3], item1[4] + item2[4]))\
                            .map(lambda x: ( x[0],x[1][0], x[1][1],x[1][2],x[1][3],x[1][4] ))

#print(delay_type.take(3))
#print("delay_type")
##saveToCSV( RDDToDF(delay_type), "./delay_type.csv" )


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

cntA = p1f1.map(lambda x: ((x[0][0], x[0][1]),1)  ).reduceByKey(lambda item1, item2: item1 + item2)
cntB = p1f2.map(lambda x: ((x[0][0], x[0][1]),1)  ).reduceByKey(lambda item1, item2: item1 + item2)

mergedACnt = cntA.map(lambda x: (x[0][0], x[0][1], x[1])  )
mergedBCnt = cntB.map(lambda x: (x[0][0], x[0][1], x[1])  )
#sumA.
#
#złączenie roku-miesiac
mergedA = sumA.map(lambda x: (x[0][0], x[0][1], x[1])  )
mergedB = sumB.map(lambda x: (x[0][0], x[0][1], x[1])  )

#print(mergedA.take(10))
#saveToCSV(RDDToDF(mergedA),"./mergedA_del.csv")
#saveToCSV(RDDToDF(mergedB),"./mergedB_del.csv")


#print(mergedACnt.take(10))
##saveToCSV(RDDToDF(mergedACnt),"./mergedACnt_del.csv")
##saveToCSV(RDDToDF(mergedBCnt),"./mergedBCnt_del.csv")




# Ilosc lotów w ciagu miesiąca kazdego roku
# 1) Z clean bierzemy wszytskie elementy - tworzymy date z 1: ((x[0] ,x[1]) , 1)
# 2) Zliczamy 1 i zapisujemy

AllFlights = clean.map(lambda x: ((x[0], x[2]), 1))
AllFlightsPerMonthAndYear = AllFlights.reduceByKey(lambda item1, item2 :  item1 + item2)
mergedAllFlights = AllFlightsPerMonthAndYear.map(lambda x: (x[0][0], x[0][1], x[1])  )

print("AllFlights")
##saveToCSV( RDDToDF(mergedAllFlights), "./AllFlights.csv" )
print("AllFlights")




# Najpopularniejsze destynacje ogólnie
# 1) Pobranie danych. Zapis w postaci (miasto, 1)
#       25 -> Destination City Name

# 2) Zliczenie wystąpień.
City = clean.map(lambda x : (x[25], 1))
popularDest = City.reduceByKey(lambda item1, item2: item1 + item2)
#sortedPopDest = popularDest.sortBy(lambda a: -a[1])
print("popularDest")
##saveToCSV( RDDToDF(popularDest), "./popularDest.csv" )

popularDestDel = clean.map(lambda x : (x[25], x[44]))\
                        .filter(lambda x: x[1]!= "" )\
                        .map(lambda x: (x[0], float(x[1]) ))\
                        .filter(lambda x: x[1] > 5)\
                        .map(lambda x: (x[0], 1))\
                        .reduceByKey(lambda item1, item2: item1 + item2)
##saveToCSV( RDDToDF(popularDestDel), "./popularDestDel.csv" )      #QWE



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

#SUMA opoźnień dla kazego linii lotniczej
delayAirlineArr = a1f2.reduceByKey(lambda item1, item2 :  item1 + item2)
print("delayAirlineArr")
saveToCSV( RDDToDF(delayAirlineArr), "./delayAirlineArr.csv" )        #QWE
print("delayAirlineArr")


##saveToCSV( RDDToDF(airlineDelay1), "./AirlinesFlightsDelay1.csv" )
print("airlineDelay1 ", time.time() - now )

##saveToCSV( RDDToDF(airlineDelay2), "./AirlinesFlightsDelay2.csv" )        #QWE
print("airlineDelay2 ", time.time() - now )

# ogolnie ile linie mialy lotow

allFlightsAirline = clean.map(lambda x: (x[6], 1 ))

allFlightsAirlineCNT = allFlightsAirline.reduceByKey(lambda item1, item2 :  item1 + item2)

now = time.time()
##saveToCSV( RDDToDF(allFlightsAirlineCNT), "./allFlightsAirlineCNT.csv" )      #QWE
print("allFlightsAirlineCNT ", time.time() - now )

input("KONIEC ")

sc.stop()