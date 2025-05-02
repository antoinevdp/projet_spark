from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
import sys

# Définir explicitement les variables d'environnement
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_202'

# Créer une SparkSession à partir du SparkContext existant
spark = SparkSession.builder \
    .appName("HiveTableStorage") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.ui.port", "4040") \
    .config("spark.jars", "src\mysql-connector-j-9.3.0.jar") \
    .config("hive.stats.jdbc.timeout", "80") \
    .config("hive.stats.retries.wait", "30") \
    .config("hive.security.authorization.enabled", "false") \
    .config("hive.metastore.schema.verification", "false") \
    .config("spark.sql.hive.metastore.version", "2.3.9") \
    .config("spark.sql.hive.metastore.jars", "builtin") \
    .config("spark.sql.hive.metastore.sharedPrefixes", "org.mariadb.jdbc,com.mysql.cj.jdbc") \
    .config("spark.hadoop.hive.metastore.schema.verification", "false") \
    .config("spark.hadoop.hive.metastore.schema.verification.record.version", "false") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Vérifier que la SparkSession a bien le support Hive
print("Support Hive activé :", spark.conf.get("spark.sql.catalogImplementation") == "hive")

sc = spark.sparkContext
print("SparkContext créé avec succès")

# Chemin de sortie pour les fichiers Parquet'
input_dir = "bronze"
parquet_file = "Meteo.parquet"

def import_parquet(input_dir,parquet_file):
    # Lire le fichier Parquet
    df = spark.read.parquet(input_dir + "/"+ parquet_file)   
    return df

def VueTempSparkSQL(df, nametable):
    # Créer une vue temporaire pour exécuter des requêtes SQL
    return df.createOrReplaceTempView(nametable)

def moyenne(df,namenewcolonne,colonneliste):
    # Calculer la moyenne des colonnes spécifiées
    for i, colonne in enumerate(colonneliste):
        if i == 0:
         # Pour la première colonne, initialiser l'expression
            expr = when(col(colonne) != 0, col(colonne))
        else:
            # Pour les colonnes suivantes, ajouter à l'expression
            expr = expr.when(col(colonne) != 0, col(colonne))

    df = df.withColumn(namenewcolonne,moyenne)
    return df
    
    # Vitesse du vent à différentes hauteurs
wind_speed = ['wind_speed_10m', 'wind_speed_80m', 'wind_speed_120m', 'wind_speed_180m']

# Direction du vent à différentes hauteurs
wind_direction = ['wind_direction_10m', 'wind_direction_80m', 'wind_direction_120m', 'wind_direction_180m']

# Température de l'air à différentes hauteurs
air_temperature = ['temperature_80m', 'temperature_120m', 'temperature_180m']

# Température du sol à différentes profondeurs
soil_temperature = ['soil_temperature_0cm', 'soil_temperature_6cm', 'soil_temperature_18cm', 'soil_temperature_54cm']

# Humidité du sol à différentes profondeurs
soil_moisture = ['soil_moisture_0_to_1cm', 'soil_moisture_1_to_3cm', 'soil_moisture_3_to_9cm', 'soil_moisture_9_to_27cm', 'soil_moisture_27_to_81cm']


Meteo_brute = VueTempSparkSQL(import_parquet(input_dir,parquet_file),"Meteo")
print("Table Meteo créée avec succès")
print("Affichage des 5 premières lignes du DataFrame")
Meteo_brute.show(5)
meteo_myenne1 = moyenne(Meteo_brute,"moyenne_wind_speed",wind_speed)
meteo_myenne2 = moyenne(meteo_myenne1,"moyenne_wind_direction",wind_direction)
meteo_myenne3 = moyenne(meteo_myenne2,"moyenne_air_temperature",air_temperature)
meteo_myenne4 = moyenne(meteo_myenne3,"moyenne_soil_temperature",soil_temperature)
meteo_myenne5 = moyenne(meteo_myenne4,"moyenne_soil_moisture",soil_moisture)
print("Affichage des 5 premières lignes du DataFrame")
meteo_myenne5.show(5)
