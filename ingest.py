import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingest").getOrCreate()

URL = f"https://api.aviationstack.com/v1/airports?access_key=8169fc0b517e5cdf325278ae336bede4&country_iso2=FR"

response = requests.get(URL)
data = response.json()

airports = data['data']
total = data['pagination']['total']
left = total - data['pagination']['count']
for offset in range(100,total,100):
    if left > 0:
        response = requests.get(
            f"{URL}&offset={offset}")
        data = response.json()
        airports.extend(data['data'])
        left = total - data['pagination']['count']


df = spark.createDataFrame(airports)

df.write.format("parquet").save("bronze/airports.parquet")