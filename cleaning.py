from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("cleaning").getOrCreate()

df = spark.read.parquet("bronze/airports.parquet")

df.createOrReplaceTempView("airports")

query = spark.sql("select * from airports WHERE city_iata_code = 'PAR' ")
query.show()