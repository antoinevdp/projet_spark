from os.path import abspath
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

warehouse_location = abspath('silver-warehouse')

load_dotenv()

jdbc_url = "jdbc:mysql://localhost:3306/flights"
properties = {
    "user": os.getenv('USER'),
    "password": os.getenv('PASSWORD'),
    "driver": "com.mysql.cj.jdbc.Driver",
}

def read_dataframe(spark):
    weather_df = spark.read.parquet(warehouse_location + '/weather')
    flights_df = spark.read.parquet(warehouse_location + '/flights')
    return weather_df, flights_df


def average_delay_by_company(spark, df):
    df.createOrReplaceTempView("flights")
    query = """SELECT Airline, AVG(ArrDelayMinutes) FROM flights GROUP BY Airline ORDER BY AVG(ArrDelayMinutes) DESC"""
    df = spark.sql(query)
    return df

def top_delay_during_snow(spark, flights_df, weather_df):
    flights_df.createOrReplaceTempView("flights")
    weather_df.createOrReplaceTempView("weather")

    query = """
            SELECT f.Airline, f.Origin, f.Dest, f.diverted,f.DepDelayMinutes, f.ArrDelayMinutes, w.snowfall, w.snow_depth,
            w.wind_speed_10m, w.wind_direction_10m, w.wind_gusts_10m, w.precipitation, w.cloud_cover
            FROM flights as f
            LEFT JOIN weather as w
            ON f.Origin = w.location AND f.CRSDepTime_hourly = w.datetimeUTC
            WHERE w.weather_code IN (71,73,75)
            ORDER BY f.ArrDelayMinutes DESC
        """
    df = spark.sql(query)
    return df

def top_delay_by_weather(spark, flights_df, weather_df):
    flights_df.createOrReplaceTempView("flights")
    weather_df.createOrReplaceTempView("weather")

    query = """
                SELECT w.weather_code,f.Airline, MAX(f.ArrDelayMinutes)
                FROM flights as f
                LEFT JOIN weather as w
                ON f.Origin = w.location AND f.CRSDepTime_hourly = w.datetimeUTC
                GROUP BY w.weather_code, f.Airline
                ORDER BY MAX(f.ArrDelayMinutes) DESC
            """

    df = spark.sql(query)
    return df


def top_delay_by_airline(spark, flights_df):
    flights_df.createOrReplaceTempView("flights")

    query = """
                SELECT f.Airline, MAX(f.ArrDelayMinutes)
                FROM flights as f
                GROUP BY f.Airline 
                ORDER BY MAX(f.ArrDelayMinutes) DESC
            """

    df = spark.sql(query)
    return df

def number_of_flights_by_airline(spark, flights_df):
    flights_df.createOrReplaceTempView("flights")

    query = """
                SELECT *, CAST(total_delayed_flights/total_fligths AS double) as ratio FROM (
                SELECT f.Airline, COUNT(*) as total_fligths, sum(case when f.ArrDelayMinutes> 0 then 1 else 0 end) as total_delayed_flights
                FROM flights as f
                GROUP BY f.Airline
                )
                ORDER BY ratio DESC
            """

    df = spark.sql(query)
    return df


def save_to_mysql(spark, df, table_name, properties):
    df.write \
        .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)




if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Cleaning") \
        .config("spark.sql.warehouse.dir", warehouse_location).master("spark://localhost:7077") \
        .config("spark.executor.memory", "16g").config("spark.executor.core", "8")\
        .config("spark.jars", abspath("mysql-connector-j-9.3.0.jar")).enableHiveSupport().getOrCreate())

    print(spark.sparkContext._conf.get("spark.jars"))

    weather_df, flights_df = read_dataframe(spark)
    df1 = average_delay_by_company(spark, flights_df)
    df2 = top_delay_during_snow(spark, flights_df, weather_df)
    df3 = top_delay_by_weather(spark, flights_df, weather_df)
    df4 = top_delay_by_airline(spark, flights_df)
    df5 = number_of_flights_by_airline(spark, flights_df)

    save_to_mysql(spark, df1, "average_delay_by_company", properties)
    save_to_mysql(spark, df2, "top_delay_during_snow", properties)
    save_to_mysql(spark, df3, "top_delay_by_weather", properties)
    save_to_mysql(spark, df4, "top_delay_by_airline", properties)
    save_to_mysql(spark, df5, "number_of_flights_by_airline", properties)
