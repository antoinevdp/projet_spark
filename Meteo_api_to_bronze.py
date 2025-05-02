import openmeteo_requests
from datetime import datetime
import requests_cache
import os
import time
import sys
from retry_requests import retry
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql.functions import when, isnan, col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window



def get_Spark_data_spark(latitude=48.85, longitude=2.35, date_depart=None, date_end=None, hourly_variables=None, sparkSess=None):
    """
    Récupère les données météorologiques de l'API Open-Meteo et les convertit en DataFrame Spark.
    
    Args:
        latitude (float): Latitude de l'emplacement
        longitude (float): Longitude de l'emplacement
        past_days (int): Nombre de jours passés à inclure
        forecast_days (int): Nombre de jours de prévision à inclure
        hourly_variables (list): Liste des variables horaires à récupérer. Si None, utilise une liste par défaut
        sparkSess (SparkSession): Session Spark active
        
    Returns:
        spark.DataFrame: DataFrame Spark contenant les données météorologiques horaires
    """
    # Variables horaires par défaut
    if hourly_variables is None:
        hourly_variables = [
            "visibility", "snowfall", "snow_depth", "rain", "wind_speed_10m",
            "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", 
            "wind_direction_10m", "wind_direction_120m", "wind_direction_80m", "wind_direction_180m",
            "temperature_80m", "temperature_120m", "temperature_180m",
            "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm",
            "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", 
            "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm",
            "wind_gusts_10m", "precipitation", "apparent_temperature",
            "cloud_cover_mid", "cloud_cover_high", "cloud_cover_low", "sunshine_duration",
            "is_day", "uv_index", "cloud_cover", "surface_pressure", "pressure_msl", "weather_code"
        ]
    
    # Configuration de la session pour l'APIr
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    # Paramètres de requête API
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date_depart,
	    "end_date": date_end,
        "hourly": hourly_variables,
        "models": "best_match"
    }
    
    # Appel à l'API
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    
    # Préparation du dictionnaire pour les données horaires
    hourly = response.Hourly()
    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}
    
    # Remplissage des données pour chaque variable
    for i in range(len(hourly_variables)):
        values = hourly.Variables(i).ValuesAsNumpy()
        hourly_data[hourly_variables[i]] = values
    
    # Création du DataFrame pandas
    hourly_dataframe_pd = pd.DataFrame(data=hourly_data)
    
    # Par exemple, pour les colonnes avec des types mixtes, convertissez-les en string
    for col in hourly_dataframe_pd.columns:
        if hourly_dataframe_pd[col].dtype == 'object':
            hourly_dataframe_pd[col] = hourly_dataframe_pd[col].astype(str)

    # Conversion en DataFrame Spark
    hourly_dataframe_spark = sparkSess.createDataFrame(hourly_dataframe_pd)

    
    return hourly_dataframe_spark

def get_france_weather_spark(date_depart=None, date_end=None, french_cities=None, sparkSess=None):
    hourly_variables = [
            "visibility", "snowfall", "snow_depth", "rain", "wind_speed_10m",
            "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", 
            "wind_direction_10m", "wind_direction_120m", "wind_direction_80m", "wind_direction_180m",
            "temperature_80m", "temperature_120m", "temperature_180m",
            "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm",
            "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", 
            "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm",
            "wind_gusts_10m", "precipitation", "apparent_temperature",
            "cloud_cover_mid", "cloud_cover_high", "cloud_cover_low", "sunshine_duration",
            "is_day", "uv_index", "cloud_cover", "surface_pressure", "pressure_msl", "weather_code"
        ]
    all_meteo = None
    for city,lat,lon in french_cities:
        meteo_ville = get_Spark_data_spark(latitude=lat, longitude=lon, date_depart=date_depart, date_end=date_end, hourly_variables=None, sparkSess=sparkSess)
        meteo_ville  = meteo_ville.withColumn("city", F.lit(city))
        meteo_ville = meteo_ville.withColumn("latitude", F.lit(lat))  
        meteo_ville = meteo_ville.withColumn("longitude", F.lit(lon))
        #time.sleep(60)
        # Si c'est la première ville, initialiser all_meteo avec ce DataFrame
        if all_meteo is None:
            all_meteo = meteo_ville
        else:
            # Sinon, faire l'union
            all_meteo = all_meteo.union(meteo_ville)

    return all_meteo

def add_date_features(Spark_df):
    """
    Ajoute des caractéristiques temporelles au DataFrame météo.
    Gère différents types de données et nettoie les valeurs problématiques.
    
    Args:
        Spark_df: DataFrame Spark contenant les données météo
        
    Returns:
        DataFrame Spark avec des caractéristiques temporelles ajoutées et nettoyé
    """
    from pyspark.sql.functions import col, when, isnan, lit
    from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType, DateType
    
    # Vérifier et convertir le champ date en timestamp si nécessaire
    if "date" in Spark_df.columns:
        date_type = Spark_df.schema["date"].dataType
        if not isinstance(date_type, TimestampType) and not isinstance(date_type, DateType):
            Spark_df = Spark_df.withColumn("date", col("date").cast(TimestampType()))
    
    # Ajouter les caractéristiques temporelles
    result_df = Spark_df \
        .withColumn("year", F.year("date").cast(IntegerType())) \
        .withColumn("month", F.month("date").cast(IntegerType())) \
        .withColumn("day", F.dayofmonth("date").cast(IntegerType())) \
        .withColumn("hour", F.hour("date").cast(IntegerType())) \
        .withColumn("dayofweek", F.dayofweek("date").cast(IntegerType())) \
        .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0).cast(IntegerType())) \
        .withColumn("season", F.when(F.month("date").isin(3, 4, 5), "spring")
                             .when(F.month("date").isin(6, 7, 8), "summer")
                             .when(F.month("date").isin(9, 10, 11), "autumn")
                             .otherwise("winter").cast(StringType()))
    
    # Nettoyer toutes les colonnes numériques pour éviter les problèmes lors de l'insertion dans MySQL
    numeric_columns = [f.name for f in result_df.schema.fields 
                      if str(f.dataType).startswith("Double") or 
                         str(f.dataType).startswith("Float") or 
                         str(f.dataType).startswith("Int") or 
                         str(f.dataType).startswith("Long")]
    
    # Remplacer les valeurs NaN et null par 0 dans les colonnes numériques
    for column_name in numeric_columns:
        result_df = result_df.withColumn(
            column_name,
            when(col(column_name).isNull() | isnan(col(column_name)), 0).otherwise(col(column_name))
        )
    
    # S'assurer que toutes les colonnes de chaînes de caractères ont des valeurs non nulles
    string_columns = [f.name for f in result_df.schema.fields if str(f.dataType).startswith("String")]
    for column_name in string_columns:
        result_df = result_df.withColumn(
            column_name,
            when(col(column_name).isNull(), "").otherwise(col(column_name))
        )
    
    return result_df

def insert_Bronzelayer(df,server_name, port , table_name, mode, properties):
    # URL JDBC
    url = f"jdbc:mysql://{server_name}:{port}/bronze"
    # Insert the BDD layer into the model
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

french_cities = [
    ("Paris", 48.85, 2.35),  # Aéroports Charles-de-Gaulle et Orly
    ("Marseille", 43.30, 5.37),  # Aéroport Marseille-Provence
    ("Lyon", 45.76, 4.84),  # Aéroport Lyon-Saint Exupéry
    ("Toulouse", 43.60, 1.44),  # Aéroport Toulouse-Blagnac
    ("Nice", 43.70, 7.27),  # Aéroport Nice-Côte d'Azur
    ("Nantes", 47.22, -1.55),  # Aéroport de Nantes
    ("Strasbourg", 48.58, 7.75),  # Aéroport de Strasbourg
    ("Montpellier", 43.61, 3.87),  # Aéroport Montpellier Méditerranée
    ("Bordeaux", 44.84, -0.58),  # Aéroport de Bordeaux
    ("Lille", 50.63, 3.07),  # Aéroport de Lille
    ("Rennes", 48.11, -1.68),  # Aéroport de Rennes
    ("Reims", 49.26, 4.03),  # Aérodrome de Reims-Prunay
    ("Le Havre", 49.49, 0.11),  # Aéroport Le Havre-Octeville
    ("Saint-Étienne", 45.44, 4.39),  # Aéroport Saint-Étienne-Bouthéon
    ("Toulon", 43.12, 5.93),  # Aéroport Toulon-Hyères
    ("Grenoble", 45.19, 5.73),  # Aéroport Grenoble-Alpes-Isère
    ("Dijon", 47.32, 5.04),  # Aéroport Dijon-Bourgogne
    ("Angers", 47.48, -0.56),  # Aérodrome d'Angers-Loire
    ("Nîmes", 43.84, 4.36),  # Aéroport Nîmes-Alès-Camargue-Cévennes
    ("Villeurbanne", 45.77, 4.88),  # Pas d'aéroport propre (utilise Lyon)
    ("Clermont-Ferrand", 45.78, 3.08),  # Aéroport Clermont-Ferrand Auvergne
    ("Le Mans", 48.00, 0.20),  # Aérodrome Le Mans-Arnage
    ("Aix-en-Provence", 43.53, 5.45),  # Pas d'aéroport propre (utilise Marseille)
    ("Brest", 48.39, -4.49),  # Aéroport Brest Bretagne
    ("Tours", 47.39, 0.69),  # Aéroport Tours Val de Loire
    ("Limoges", 45.83, 1.26),  # Aéroport Limoges-Bellegarde
    ("Amiens", 49.89, 2.30),  # Aérodrome d'Amiens-Glisy
    ("Annecy", 45.90, 6.13),  # Aéroport Annecy Mont-Blanc
    ("Perpignan", 42.70, 2.90),  # Aéroport Perpignan-Rivesaltes
    ("Besançon", 47.24, 6.02),  # Aérodrome de Besançon-La Vèze
    ("Metz", 49.12, 6.18),  # Aéroport Metz-Nancy-Lorraine
    ("Orléans", 47.90, 1.90),  # Aéroport Orléans-Saint-Denis-de-l'Hôtel
    ("Rouen", 49.44, 1.10),  # Aéroport Rouen Vallée de Seine
    ("Mulhouse", 47.75, 7.34),  # EuroAirport Bâle-Mulhouse-Fribourg
    ("Caen", 49.18, -0.37),  # Aéroport Caen-Carpiquet
    ("Nancy", 48.69, 6.18),  # Aéroport Metz-Nancy-Lorraine
    ("Argenteuil", 48.95, 2.25),  # Pas d'aéroport propre (utilise Paris)
    ("Saint-Denis", 48.94, 2.36),  # Pas d'aéroport propre (utilise Paris)
    ("Roubaix", 50.69, 3.18),  # Pas d'aéroport propre (utilise Lille)
    ("Tourcoing", 50.72, 3.16),  # Pas d'aéroport propre (utilise Lille)
    ("Avignon", 43.95, 4.81),  # Aéroport Avignon-Provence
    ("Pau", 43.30, -0.37),  # Aéroport Pau-Pyrénées
    ("Poitiers", 46.58, 0.34),  # Aéroport Poitiers-Biard
    ("La Rochelle", 46.16, -1.15),  # Aéroport La Rochelle-Île de Ré
    ("Chambéry", 45.57, 5.92),  # Aéroport Chambéry-Savoie
    ("Bayonne", 43.49, -1.47),  # Aéroport de Biarritz-Pays Basque
    ("Troyes", 48.30, 4.09),  # Aérodrome de Troyes-Barberey
    ("Dunkerque", 51.04, 2.38),  # Aérodrome de Dunkerque-Les Moëres
    ("Valence", 44.93, 4.89),  # Aéroport Valence-Chabeuil
    ("Saint-Nazaire", 47.28, -2.21)  # Aérodrome de Saint-Nazaire-Montoir
]

french_citiesmini = [
    ("Paris", 48.85, 2.35),  # Aéroports Charles-de-Gaulle et Orly
    ("Marseille", 43.30, 5.37),  # Aéroport Marseille-Provence
    ("Lyon", 45.76, 4.84),  # Aéroport Lyon-Saint Exupéry
    ("Toulouse", 43.60, 1.44),  # Aéroport Toulouse-Blagnac
    ("Nice", 43.70, 7.27),  # Aéroport Nice-Côte d'Azur
    ("Nantes", 47.22, -1.55),  # Aéroport de Nantes
    ("Strasbourg", 48.58, 7.75),  # Aéroport de Strasbourg
    ("Montpellier", 43.61, 3.87),  # Aéroport Montpellier Méditerranée
    ("Bordeaux", 44.84, -0.58),  # Aéroport de Bordeaux
    ("Lille", 50.63, 3.07),  # Aéroport de Lille
    ("Rennes", 48.11, -1.68),  # Aéroport de Rennes
    ("Reims", 49.26, 4.03),  # Aérodrome de Reims-Prunay
    ("Le Havre", 49.49, 0.11),  # Aéroport Le Havre-Octeville
    ("Saint-Étienne", 45.44, 4.39),  # Aéroport Saint-Étienne-Bouthéon
    ("Toulon", 43.12, 5.93),  # Aéroport Toulon-Hyères
    ("Grenoble", 45.19, 5.73),  # Aéroport Grenoble-Alpes-Isère
    ("Dijon", 47.32, 5.04),  # Aéroport Dijon-Bourgogne
    ("Angers", 47.48, -0.56),  # Aérodrome d'Angers-Loire
    ("Nîmes", 43.84, 4.36)
]

# Paramètres de connexion
server_name = "127.0.0.1"
port = "3306"
username = "root"
password = "mp0117MIA!"  # ton mp ici

# Options de connexion
connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.mysql.cj.jdbc.Driver"  # Driver pour MySQL 8.0+
}
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
""" 
importation des datas meteo depuis l'API 
Traitement des données météo
incertion dans la BDD bronze(my sql bronze)
avec Spark
"""

# Importer les données météo depuis l'API
print("debut importation des données météo")
meteo = get_france_weather_spark(date_depart="2017-01-01", date_end="2018-01-01",french_cities=french_citiesmini, sparkSess=spark)
print("Données météo récupérées avec succès")

print("debut traitement des types des colonnes avant incertion météo")
meteo  = add_date_features(meteo)
print("fin traitement typages des colonns météo")

print("debut import Données météo dans le bronze(MySQL)")
# Insérer les données météo dans la base de données bronze
insert_Bronzelayer(meteo,server_name, port, "meteo", "append", connection_properties)
print("Données météo insérées avec succès dans la base de données bronze")
