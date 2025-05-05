from itertools import chain
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, when, format_string, lpad, concat_ws, to_timestamp, to_utc_timestamp, \
    create_map, lit, date_trunc, expr
import time

warehouse_location = abspath('silver-warehouse')

def read_dataframe(spark):
    df_flights = spark.read.parquet(abspath("bronze/flights"), inferSchema=True)
    df_weather = spark.read.parquet(abspath("bronze/weather"))
    return df_flights, df_weather

def cleaning_flight(spark, df):

    df.createOrReplaceTempView("flights")

    time_columns = ["CRSDepTime","DepTime","CRSArrTime","ArrTime"]
    timezone = {
        "ATL":'America/New_York',
        "LAX":'America/Los_Angeles',
        "DEN":'America/Denver',
        'ORD':"America/Chicago",
    }
    # Convert Python dict to Spark MapType column
    mapping_expr = create_map([lit(k) for k in chain(*timezone.items())])

    df = df.select("FlightDate","Year","Month","Day","Airline","Origin","Dest","Cancelled","Diverted","CRSDepTime","DepTime","CRSArrTime","ArrTime","DepDelayMinutes","ArrDelayMinutes","AirTime","CRSElapsedTime","ActualElapsedTime")
    #selecting only theses 3 airports to reduce the dataset size
    df = df.where("Origin IN ('ATL','LAX','DEN','ORD') AND  Dest IN ('ATL','LAX','DEN','ORD')")

    # Add a column with the timezone based on origin
    df = df.withColumn("OriginTimezone", mapping_expr[col("Origin")])
    df = df.withColumn("DestTimezone", mapping_expr[col("Dest")])

    for column in time_columns:
        df = df.withColumn(column, col(column).cast("int"))
        df= df.withColumn(column, format_string(
            "%s:%s",
            lpad((col(column) / 100).cast("int").cast("string"), 2, "0"),
            lpad((col(column) % 100).cast("string"), 2, "0")
        ))
        df= df.withColumn(column, concat_ws(" ", col("FlightDate"), col(column)))
        df = df.withColumn(column, to_timestamp(col(column), "yyyy-MM-dd HH:mm"))
        if column in ["CRSDepTime", "DepTime"]:
            df = df.withColumn(column,to_utc_timestamp(col(column), col("OriginTimezone")))
        elif column in ["CRSArrTime", "ArrTime"]:
            df = df.withColumn(column,to_utc_timestamp(col(column), col("DestTimezone")))
        df = df.withColumn(
            f"{column}_hourly",date_trunc("hour", expr(f"{column} + interval 30 minutes"))
        )
    return df


def cleaning_weather(spark, df):

    count_rows = df.count()
    # Calcul du nombre de valeurs nulles par colonne
    df_null_counts = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])
    # Extraction des colonnes 100 % nulles
    columns_null = [c for c in df.columns if df_null_counts.select(c).first()[0] == count_rows]
    print(columns_null)
    df = df.drop(*columns_null)
    df = df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm"))
    df = df.withColumn("datetimeUTC", to_utc_timestamp(col("datetime"), col("timezone")))

    return df

def save_dataframe(spark, df,name):
    df.write.format("parquet").mode("overwrite").partitionBy("Year", "Month", "Day").saveAsTable(name)



if __name__ == "__main__":
    spark = SparkSession.builder.appName("Cleaning") \
        .config("spark.sql.warehouse.dir", warehouse_location).master("spark://localhost:7077") \
        .config("spark.executor.memory", "16g").config("spark.executor.core", "8").enableHiveSupport().getOrCreate()


    df_flights, df_weather = read_dataframe(spark)
    df_weather = cleaning_weather(spark, df_weather)
    df_flights = cleaning_flight(spark, df_flights)
    save_dataframe(spark, df_flights, "flights")
    save_dataframe(spark, df_weather, "weather")


