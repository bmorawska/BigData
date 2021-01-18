from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import time
import os
import shutil

print("Spark Configuration...")
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
spark = SparkSession(sc)
print("SparkContext initiated ")

now = time.time()
header = sc.textFile("./header.csv")
file_rdd = sc.textFile(r"C:\Basia\Szkola\studia\rok_5\semestr_9\Przetwarzanie_i_analiza_duzych_zbiorow_danych"
                       r"\projekt_samoloty\BIGDATA\*.csv")

# file_rdd = sc.textFile('../../test/*.csv')
loaded = time.time() - now
print("LOADED ", loaded)

now = time.time()
h = header.first()
file_rdd = file_rdd.map(lambda x: x).filter(lambda row: row != h)  # delete
print("header ", time.time() - now)

now = time.time()
rows = file_rdd.map(lambda line: line.replace('"', ''))
print("replace ", time.time() - now)

now = time.time()
rows = rows.map(lambda line: line.split(","))
clean = rows.map(lambda row: row)
print("clean ", time.time() - now)


#0  -> year
#2  -> month
#3  -> day
#33 -> departure delay (min)
#44 -> arrival delay (min)
#49 -> cancelled (0/1)
#51 -> diverted (0/1)
#56 -> distance
#58 -> CarrierDelay
#59 -> WeatherDelay
#60 -> NASDelay
#61 -> SecurityDelay
#62 -> LateAircraftDelay
#54 -> airtime


now = time.time()
delayFlight = clean.map(lambda x: (x[0], x[2], x[3], x[33], x[44], x[49], x[51],
                                   x[56], x[58], x[59], x[60], x[61], x[62], x[54])). \
    filter(lambda x: x[3] != ""). \
    filter(lambda x: x[4] != ""). \
    filter(lambda x: x[5] == '0.00'). \
    filter(lambda x: x[6] == "0.00")
print("notCancelled ", time.time() - now)


def replace_empty_on_zero(val):
    if val == '':
        return 0
    else:
        return float(val)


delays_key = delayFlight.map(lambda x: ((x[0], x[1], x[2]), [float(x[3]),
                                                             float(x[4]),
                                                             float(x[7]),
                                                             replace_empty_on_zero(x[8]),
                                                             replace_empty_on_zero(x[9]),
                                                             replace_empty_on_zero(x[10]),
                                                             replace_empty_on_zero(x[11]),
                                                             replace_empty_on_zero(x[12]),
                                                             replace_empty_on_zero(x[13]),
                                                             1]))
delays_key_reduced = delays_key.reduceByKey(lambda x1, x2: [x1[0] + x2[0],
                                                            x1[1] + x2[1],
                                                            x1[2] + x2[2],
                                                            x1[3] + x2[3],
                                                            x1[4] + x2[4],
                                                            x1[5] + x2[5],
                                                            x1[6] + x2[6],
                                                            x1[7] + x2[7],
                                                            x1[8] + x2[8],
                                                            x1[9] + x2[9]])


def toCSVLine(data):
    return ','.join(str(d) for d in data)

lines = delays_key_reduced.map(toCSVLine)

results_dir = './results'
if os.path.exists(results_dir):
    shutil.rmtree(results_dir)

lines.saveAsTextFile('results')

sc.stop()