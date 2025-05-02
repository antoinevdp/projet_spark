from os.path import abspath
from pyspark.sql import SparkSession


warehouse_location = abspath('silver-warehouse')

spark = SparkSession.builder.appName("Cleaning")\
.config("spark.sql.warehouse.dir", warehouse_location).master("spark://localhost:7077")\
    .config("spark.executor.memory", "16g").config("spark.executor.core", "8").enableHiveSupport().getOrCreate()


df = spark.read.parquet(abspath("bronze/flights"), inferSchema=True)

df.createOrReplaceTempView("flights")


df = df.select("FlightDate","FlightYear","FlightMonth","FlightDay","Airline","Origin","Dest","Cancelled","Diverted","DepDelayMinutes","ArrDelayMinutes","AirTime")
df = df.where("Origin IN ('ATL','LAX','DEN') AND  Dest IN ('ATL','LAX','ORD')")

df.createOrReplaceTempView("flights")
spark.sql("select Origin, Dest, COUNT(*) from flights GROUP BY Origin, Dest ORDER BY COUNT(*) DESC").show()


df.write.format("parquet").mode("overwrite").partitionBy("FlightYear","FlightMonth","FlightDay").saveAsTable("flights")

print(warehouse_location)

