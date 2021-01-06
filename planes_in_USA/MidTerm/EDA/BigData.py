import re
import sys
import itertools as it
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import time


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
    print(pdsDF)
    return pdsDF



print("Spark Configuration...")
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
spark = SparkSession(sc)
print("SparkContext initiated ")

now = time.time()
file_rdd = sc.textFile("./All/2018/2018_1.csv")
#file_rdd = sc.textFile("./All/*/*.csv")

print("No of Rows: ", file_rdd.count())

# HEADER delete
header = file_rdd.first()
now = time.time()
file_rdd = file_rdd.filter(lambda row : row != header)   #delete 

rows = file_rdd.map(lambda line : line.replace('"',''))

rows = rows.map(lambda line : line.split(","))
clean = rows.map(lambda row : row)          # Clean zawiera wiersze oddzielone przecinkami. Numery poszczegolnych kolumn są zawarte w pliku. (!!!)

print("Working...")


# Total number of flights. groupby airlines + count
# liczba wszyskich lotow przez kazda firme
div = clean.map(lambda x : (x[6], 1))       # x[6] -> linia lotnicza. (x[6], 1)
print("\n(AA + 1)\n")
print(div.take(5))

AirlinesFlights = div.reduceByKey(lambda item1, item2: item1 + item2)       # Zliczenie ilości

print("\nAA + Airline's Flights\n")
print(AirlinesFlights.take(10))

saveToCSV( RDDToDF(AirlinesFlights), "./AirlinesFlightsCNT.csv" )




#input("NEXT STEP... (Enter)")

delayFlight = clean.map(lambda x: (x[0], x[44], x[49], x[51])) #rok, Arrdelay, cancell, dir\

# notCancelled to przefiltrowane loty odwolane, przekierowane i te ktore w czas_opoznienia == "". Na koniec map => rok, czas_opoznienia
notCancelled = delayFlight.filter(lambda x : x[2] == '0.00' ).filter(lambda x : x[3] == '0.00').filter(lambda x : x[1] != "").map(lambda x: (x[0], float(x[1]) ))

# delayFlightFloat: Rok, czas_opoznienia + Filtr( czas_opoznienia > 5 )
delayFlightFloat = notCancelled.filter(lambda item: item[1] > 5.0)



#Ilość opoźnień 
# spośród opóźnionych lotów, robimy (rok, 1) a nastepnie zliczamy ilość 1. Otrzymamy wtedy ilosc opoznionych lotow na rok
now = time.time()
DelayPerYear = delayFlightFloat.map(lambda x: (x[0], 1))
DelayCntPerYear = DelayPerYear.reduceByKey(lambda item1, item2 :  item1 + item2)
print("DelayCntPerYear ", time.time() - now )
print(DelayCntPerYear.take(10))

now = time.time()
saveToCSV( RDDToDF(DelayCntPerYear), "./DelayCntPerYear.csv" )
print("SaveToFile ", time.time() - now )



#input("NEXT STEP... (Enter)")

# SUMA
# spośród opóźnionych lotów, sumujemy czas_opoznienia (czyli drugi argument)
sumOfDelayAll = delayFlightFloat.reduceByKey(lambda item1, item2 :  item1 + item2)

now = time.time()
saveToCSV( RDDToDF(sumOfDelayAll), "./sumOfDelayAll.csv" )
print("sumOfDelayAll ", time.time() - now )


# procent opoznionych lotow przez dana linie.
# Delay flight. Count of delay flight by airlines. Fliter ArrDelay > 1 (opozniony) + gruoupby(AIline, 1) + sum

delayFlight = clean.map(lambda x: (x[6], x[44], x[49], x[51])) #alines, Arrdelay, cancell, dir

notCancelled = delayFlight.filter(lambda x : x[2] == '0.00' ).filter(lambda x : x[3] == '0.00').filter(lambda x : x[1] != "").map(lambda x: (x[0], float(x[1]) ))
#filtred = delayFlightAirlines.filter(lambda x : x[2] == '0.00' ).filter(lambda x : x[3] == '0.00').filter(lambda x : x[1] != "").filter(lambda item: item[1] > 1.0) # only delay>1

delayFlightFloat = notCancelled.filter(lambda item: item[1] > 5.0)
# delayFlightFloat -> airline + ArrDelay
#filtred = delayFlightFloat.filter(lambda item: item[1] > 1.0)


#print("\nFILTRED\n")
#print(filtred.take(10))

#filtrToOne = filtred.map(lambda x: (x[0], 1))
filtrToOne = delayFlightFloat.map(lambda x: (x[0], 1))

print("\nfiltrToOne\n")
print(filtrToOne.take(10))

final = filtrToOne.reduceByKey(lambda item1, item2 :  item1 + item2)

print("\nfinal\n")
print(final.take(2))


print("\nEND\n")
saveToCSV( RDDToDF(final), "./AirlinesFlightsDelay.csv" )


# LACZNIE ILE MINUT SPOZNIENIA
sumOfDelay = notCancelled.reduceByKey(lambda item1, item2 :  item1 + item2)
saveToCSV( RDDToDF(sumOfDelay), "./SumOfDelay.csv" )


#input("NEXT STEP... (Enter)")

#Most popular dest
# city: DestCityName 

div2 = clean.map(lambda x : (x[25], 1))
print("\City + 1\n")
print(div2.take(2))

popularDest = div2.reduceByKey(lambda item1, item2: item1 + item2)

sortedPopDest = popularDest.sortBy(lambda a: -a[1])

print("\City + 234321\n")
print(sortedPopDest.take(2))

saveToCSV( RDDToDF(sortedPopDest), "./popularDest.csv" )


sc.stop()