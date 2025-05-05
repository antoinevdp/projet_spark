from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from os.path import abspath
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.sql.functions import when, col
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

warehouse_location = abspath('silver-warehouse')

def merge_dataset(spark):
    weather_df = spark.read.parquet(warehouse_location + '/weather')
    flights_df = spark.read.parquet(warehouse_location + '/flights')

    weather_df.createOrReplaceTempView('weather')
    flights_df.createOrReplaceTempView('flights')

    query = """
        SELECT f.Airline, f.Origin, f.Dest, f.diverted,f.ArrDelayMinutes, w.snowfall, w.snow_depth, w.rain,
        w.wind_speed_10m, w.wind_direction_10m, w.temperature_2m, w.wind_gusts_10m, w.precipitation,
        w.apparent_temperature, w.cloud_cover_mid, w.cloud_cover_high, w.cloud_cover_low, w.sunshine_duration,
        w.is_day, w.cloud_cover, w.surface_pressure, w.pressure_msl, w.weather_code
        FROM flights as f
        LEFT JOIN weather as w
        ON f.Origin = w.location AND f.CRSDepTime_hourly = w.datetimeUTC
    """

    df = spark.sql(query)

    df = df.dropna()

    airline_indexer = StringIndexer(inputCol="Airline", outputCol="Airline_index")
    origin_indexer = StringIndexer(inputCol="Origin", outputCol="Origin_index")
    dest_indexer = StringIndexer(inputCol="Dest", outputCol="Dest_index")

    df = df.withColumn("label", when(col("ArrDelayMinutes") > 0, 1).otherwise(0))

    feature_cols = [col for col in df.columns if col in ["Diverted","snowfall",
                                                         "snow_depth","rain","wind_speed_10m","wind_direction_10m",
                                                         "temperature_2m","wind_gusts_10m","precipitation",
                                                         "apparent_temperature","cloud_cover_mid","cloud_cover_high",
                                                         "cloud_cover_low","sunshine_duration","is_day","cloud_cover",
                                                         "surface_pressure","pressure_msl","weather_code"]]



    assembler = VectorAssembler(inputCols=feature_cols + ["Airline_index", "Origin_index", "Dest_index"], outputCol="features_assembled")
    scaler = StandardScaler(inputCol="features_assembled", outputCol="features", withStd=True, withMean=True)
    lr = LogisticRegression(featuresCol="features", labelCol="label")

    pipeline = Pipeline(stages=[airline_indexer, origin_indexer, dest_indexer, assembler, scaler, lr])

    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()

    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=paramGrid,
                              evaluator=evaluator,
                              numFolds=5)

    cv_model = crossval.fit(df)

    predictions = cv_model.transform(df)
    auc = evaluator.evaluate(predictions)
    print(f"AUC: {auc}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Cleaning") \
        .config("spark.sql.warehouse.dir", warehouse_location).master("spark://localhost:7077") \
        .config("spark.executor.memory", "16g").config("spark.executor.core", "8").enableHiveSupport().getOrCreate()

    merge_dataset(spark)