from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("cleaning").getOrCreate()

df = spark.read.csv("bronze/flights", header=True, inferSchema=True)

df.createOrReplaceTempView("flights")

query = spark.sql("select * from flights")
query.show()