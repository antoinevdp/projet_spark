import time
from os.path import abspath

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import requests

# Liste des emplacements à surveiller (aéroports US et points intermédiaires)
us_airports = [
    # Principaux aéroports
    ("ATL", 33.64, -84.43),  # ATL: Aéroport international Hartsfield-Jackson
    ("DEN", 39.86, -104.67),  # DEN: Aéroport international de Denver
    ("LAX", 33.94, -118.41),  # LAX: Aéroport international de Los Angeles
    ("ORD", 41.978, -87.904),  # ORD: Aéroport international O'Hare de Chicago
    # Points intermédiaires sur la route aérienne Denver-Los Angeles
    ("DEN-LAX-1", 37.9064, -109.2042),
    ("DEN-LAX-2", 35.9528, -113.7384),
    # Points intermédiaires sur la route aérienne Los Angeles-Chicago
    ("LAX-ORD-1", 36.5925, -108.3430),
    ("LAX-ORD-2", 39.2451, -98.2760),
    # Points intermédiaires sur la route aérienne Atlanta-Chicago
    ("ATL-ORD-1", 36.3915, -85.5764),
    ("ATL-ORD-2", 39.1431, -86.7228),
    # Points intermédiaires sur la route aérienne Denver-Chicago
    ("DEN-ORD-1", 40.5589, -99.1372),
    ("DEN-ORD-2", 41.2579, -93.6044),
    # Points intermédiaires sur la route aérienne Denver-Atlanta
    ("DEN-ATL-1", 37.8074, -97.9908),
    ("DEN-ATL-2", 35.7548, -91.3116),
    # Points intermédiaires sur la route aérienne Atlanta-Los Angeles
    ("ATL-LAX-1", 33.7390, -95.6434),
    ("ATL-LAX-2", 33.8380, -106.8568)
]

hourly_variables = [
            "visibility", "snowfall", "snow_depth", "rain", "wind_speed_10m",
            "wind_speed_80m", "wind_speed_120m", "wind_speed_180m",
            "wind_direction_10m", "wind_direction_120m", "wind_direction_80m", "wind_direction_180m",
            "temperature_2m", "temperature_80m", "temperature_120m", "temperature_180m",
            "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm",
            "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm",
            "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm",
            "wind_gusts_10m", "precipitation", "apparent_temperature",
            "cloud_cover_mid", "cloud_cover_high", "cloud_cover_low", "sunshine_duration",
            "is_day", "uv_index", "cloud_cover", "surface_pressure", "pressure_msl", "weather_code"
        ]



def get_meteo(spark, airports,hourly_variables, start_date="2018-01-01", end_date="2020-12-31"):
        schema = StructType(
            [
                StructField("datetime", StringType(), True),
                StructField("timezone", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("location", StringType(), True),
            ] + [
                StructField(col, DoubleType(), True) for col in hourly_variables
            ])

        str_hourly_variables = ",".join(hourly_variables)
        rows = []

        for airport,lat,long in airports:
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={long}&start_date={start_date}&end_date={end_date}&hourly={str_hourly_variables}&timezone=America/New_York"
            response = requests.get(url).json()

            if "error" in response and response["error"] == True:
                print(response["reason"])
                return

            hourly = response["hourly"]
            timezone = response["timezone"]
            latitude = response["latitude"]
            longitude = response["longitude"]

            for i in range(len(hourly["time"])):
                row = {}
                for col in hourly_variables:
                    value = hourly[col][i]
                    if value is None:
                        row[col] = None
                    else:
                        row[col] = float(value)
                row["datetime"] = hourly["time"][i]
                row["timezone"] = timezone
                row["latitude"] = latitude
                row["longitude"] = longitude
                row["location"] = airport
                rows.append(row)
            print(f"Fetched data of airport: {airport} for the period {start_date} to {end_date}. Including {len(rows)} rows.")
            time.sleep(30) #sleep to prevent to overload the API

        df = spark.createDataFrame(rows, schema=schema)

        df_year_month_day = df.withColumns(
            {"Year": F.year(F.col("datetime")), "Month": F.month(F.col("datetime")),
             "Day": F.dayofmonth(F.col("datetime"))})

        df_year_month_day.write.format("parquet").mode("overwrite").partitionBy("Year","Month","Day").save(abspath("bronze/weather"))

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("openmeteo-ingest")
             .master("spark://localhost:7077")
             .config("spark.executor.memory", "16g")
             .config("spark.executor.core", "8")
             .getOrCreate())

    get_meteo(spark, us_airports,hourly_variables)


