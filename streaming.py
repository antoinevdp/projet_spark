from pyspark.sql import SparkSession
from os.path import abspath
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, TimestampType, DoubleType, \
    DecimalType

spark = SparkSession.builder.appName('streaming').master("spark://localhost:7077")\
    .config("spark.executor.memory", "16g").config("spark.executor.core", "8").getOrCreate()

schema = StructType([
    StructField('FlightDate', DateType(), True),
    StructField('Airline', StringType(), True),
    StructField('Origin', StringType(), True),
    StructField('Dest', StringType(), True),
    StructField('Cancelled', BooleanType(), True),
    StructField('Diverted', BooleanType(), True),
    StructField('CRSDepTime', DecimalType(), True),
    StructField('DepTime', DoubleType(), True),
    StructField('DepDelayMinutes', DoubleType(), True),
    StructField('DepDelay', DoubleType(), True),
    StructField('ArrTime', DoubleType(), True),
    StructField('ArrDelayMinutes', DoubleType(), True),
    StructField('AirTime', DoubleType(), True),
    StructField('CRSElapsedTime', DoubleType(), True),
    StructField('ActualElapsedTime', DoubleType(), True),
    StructField('Distance', DoubleType(), True),
    StructField('Year', DecimalType(), True),
    StructField('Quarter', DecimalType(), True),
    StructField('Month', DecimalType(), True),
    StructField('DayofMonth', DecimalType(), True),
    StructField('DayOfWeek', DecimalType(), True),
    StructField('Marketing_Airline_Network', StringType(), True),
    StructField('Operated_or_Branded_Code_Share_Partners', StringType(), True),
    StructField('DOT_ID_Marketing_Airline', DecimalType(), True),
    StructField('IATA_Code_Marketing_Airline', StringType(), True),
    StructField('Flight_Number_Marketing_Airline', DecimalType(), True),
    StructField('Operating_Airline', StringType(), True),
    StructField('DOT_ID_Operating_Airline', DecimalType(), True),
    StructField('IATA_Code_Operating_Airline', StringType(), True),
    StructField('Tail_Number', StringType(), True),
    StructField('Flight_Number_Operating_Airline', DecimalType(), True),
    StructField('OriginAirportID', DecimalType(), True),
    StructField('OriginAirportSeqID', DecimalType(), True),
    StructField('OriginCityMarketID', DecimalType(), True),
    StructField('OriginCityName', StringType(), True),
    StructField('OriginState', StringType(), True),
    StructField('OriginStateFips', DecimalType(), True),
    StructField('OriginStateName', StringType(), True),
    StructField('OriginWac', DecimalType(), True),
    StructField('DestAirportID', DecimalType(), True),
    StructField('DestAirportSeqID', DecimalType(), True),
    StructField('DestCityMarketID', DecimalType(), True),
    StructField('DestCityName', StringType(), True),
    StructField('DestState', StringType(), True),
    StructField('DestStateFips', DecimalType(), True),
    StructField('DestStateName', StringType(), True),
    StructField('DestWac', DecimalType(), True),
    StructField('DepDel15', DoubleType(), True),
    StructField('DepartureDelayGroups', DoubleType(), True),
    StructField('DepTimeBlk', StringType(), True),
    StructField('TaxiOut', DoubleType(), True),
    StructField('WheelsOff', DoubleType(), True),
    StructField('WheelsOn', DoubleType(), True),
    StructField('TaxiIn', DoubleType(), True),
    StructField('CRSArrTime', DecimalType(), True),
    StructField('ArrDelay', DoubleType(), True),
    StructField('ArrDel15', DoubleType(), True),
    StructField('ArrivalDelayGroups', DoubleType(), True),
    StructField('ArrTimeBlk', StringType(), True),
    StructField('DistanceGroup', DecimalType(), True),
    StructField('DivAirportLandings', DecimalType(), True),
])

flights = spark.readStream.format("csv").schema(schema).option("header", True).csv("source")

flights_year_month_day = flights.withColumns({"FlightYear": F.year(F.col("FlightDate")),"FlightMonth": F.month(F.col("FlightDate")),"FlightDay": F.dayofmonth(F.col("FlightDate"))})

query = flights_year_month_day.writeStream\
.format("parquet")\
.partitionBy("FlightYear","FlightMonth","FlightDay")\
.outputMode("append") \
.option("path", abspath("bronze/flights"))\
.option("checkpointLocation", abspath("bronze/checkpoints/flights"))\
.start()
query.awaitTermination()