# Import des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType, DateType,FloatType, LongType, DecimalType
from pyspark.sql.functions import col, when, lit, expr, isnan
from pyspark.sql.window import Window
import pyarrow.parquet as pq
import pyspark.sql.functions as F
import os
import sys

# Configuration des variables d'environnement pour Spark et Java
# Nécessaire pour s'assurer que Spark utilise le bon interpréteur Python et JDK
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_202'

# Création d'une session Spark avec support Hive
# Plusieurs configurations sont définies pour optimiser le fonctionnement avec Hive
spark = SparkSession.builder \
    .appName("HiveTableStorage") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.ui.port", "4040") \
    .config("spark.jars", "projet_spark\mysql-connector-j-9.3.0.jar") \
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

# Vérification de l'activation du support Hive
print("Support Hive activé :", spark.conf.get("spark.sql.catalogImplementation") == "hive")

# Récupération du contexte Spark
sc = spark.sparkContext
print("SparkContext créé avec succès")

# Définition des chemins pour les données bronze (entrée) et silver (sortie)
input_dirbronze = "bronze"
output_dirsilver = "silver"
parquet_file = "Meteo.parquet"

def import_parquet(input_dir, parquet_file):
    """
    Fonction pour importer un fichier Parquet depuis un répertoire spécifique
    
    Args:
        input_dir (str): Répertoire d'entrée
        parquet_file (str): Nom du fichier Parquet
        
    Returns:
        DataFrame: DataFrame Spark contenant les données du fichier Parquet
    """
    # Lire le fichier Parquet - le motif '*/' permet de chercher dans tous les sous-dossiers
    df = spark.read.parquet(input_dir + "/*/"+ parquet_file)   
    return df

def VueTempSparkSQL(df, nametable):
    """
    Crée une vue temporaire pour pouvoir utiliser SQL sur le DataFrame
    
    Args:
        df (DataFrame): DataFrame à transformer en vue
        nametable (str): Nom de la vue temporaire
        
    Returns:
        None: Crée simplement la vue temporaire
    """
    return df.createOrReplaceTempView(nametable)

def convert_datatype(Spark_df):
    """
    Convertit les types de données et ajoute des colonnes temporelles dérivées
    
    Args:
        Spark_df (DataFrame): DataFrame d'entrée
        
    Returns:
        DataFrame: DataFrame avec types convertis et colonnes temporelles ajoutées
    """
    # Convertir la colonne datetime en TimestampType si nécessaire
    if "datetime" in Spark_df.columns:
        date_type = Spark_df.schema["datetime"].dataType
        if not isinstance(date_type, TimestampType) and not isinstance(date_type, DateType):
            Spark_df = Spark_df.withColumn("datetime", col("datetime").cast(TimestampType()))
    
    # Ajouter des colonnes temporelles dérivées (année, mois, jour, etc.)
    result_df = Spark_df \
        .withColumn("year", F.year("datetime").cast(IntegerType())) \
        .withColumn("month", F.month("datetime").cast(IntegerType())) \
        .withColumn("day", F.dayofmonth("datetime").cast(IntegerType())) \
        .withColumn("hour", F.hour("datetime").cast(IntegerType())) \
        .withColumn("dayofweek", F.dayofweek("datetime").cast(IntegerType())) \
        .withColumn("is_weekend", F.when(F.dayofweek("datetime").isin(1, 7), 1).otherwise(0).cast(IntegerType())) \
        .withColumn("season", F.when(F.month("datetime").isin(3, 4, 5), "spring")
                             .when(F.month("datetime").isin(6, 7, 8), "summer")
                             .when(F.month("datetime").isin(9, 10, 11), "autumn")
                             .otherwise("winter").cast(StringType()))

    # Identifier toutes les colonnes numériques
    numeric_columns = [f.name for f in result_df.schema.fields 
                      if str(f.dataType).startswith("Double") or 
                         str(f.dataType).startswith("Float") or 
                         str(f.dataType).startswith("Int") or 
                         str(f.dataType).startswith("Long")]

    # Remplacer les valeurs NULL ou NaN par 0 dans les colonnes numériques
    for column_name in numeric_columns:
        result_df = result_df.withColumn(
            column_name,
            when(col(column_name).isNull() | isnan(col(column_name)), 0).otherwise(col(column_name))
        )

    # Remplacer les valeurs NULL par des chaînes vides dans les colonnes de type String
    string_columns = [f.name for f in result_df.schema.fields if str(f.dataType).startswith("String")]
    for column_name in string_columns:
        result_df = result_df.withColumn(
            column_name,
            when(col(column_name).isNull(), "").otherwise(col(column_name))
        )
    
    return result_df

def moyenne(df, namenewcolonne: str, colonneliste: list):
    """
    Calcule la moyenne de plusieurs colonnes et l'ajoute comme nouvelle colonne
    Ignore les valeurs 0 dans le calcul de la moyenne
    
    Args:
        df (DataFrame): DataFrame d'entrée
        namenewcolonne (str): Nom de la nouvelle colonne contenant la moyenne
        colonneliste (list): Liste des colonnes à moyenner
        
    Returns:
        DataFrame: DataFrame avec la nouvelle colonne de moyenne
    """
    sum_expr = lit(0)  # Initialisation de l'expression pour la somme
    count_expr = lit(0)  # Initialisation de l'expression pour le compte
    
    # Pour chaque colonne, ajouter sa valeur à la somme et incrémenter le compteur si la valeur n'est pas 0
    for colonne in colonneliste:
        sum_expr = sum_expr + when(col(colonne) != 0, col(colonne)).otherwise(0)
        count_expr = count_expr + when(col(colonne) != 0, 1).otherwise(0)
    
    # Calculer la moyenne (somme / compteur) si le compteur est supérieur à 0, sinon 0
    avg_expr = when(count_expr > 0, sum_expr / count_expr).otherwise(0)
    
    return df.withColumn(namenewcolonne, avg_expr)

def suppcolzero(df):
    """
    Supprime les colonnes où toutes les valeurs sont 0 ou vides
    
    Args:
        df (DataFrame): DataFrame d'entrée
        
    Returns:
        DataFrame: DataFrame sans les colonnes où toutes les valeurs sont 0 ou vides
    """
    colonnes = df.columns
    colonnes_a_conserver = []

    schema = df.schema
    
    # Pour chaque colonne, vérifier son type et si elle contient des valeurs non nulles/non vides
    for colonne in colonnes:
        data_type = [field.dataType for field in schema.fields if field.name == colonne][0]
        
        # Pour les colonnes numériques, vérifier si elles contiennent des valeurs non-zéro
        if isinstance(data_type, (IntegerType, DoubleType, FloatType, LongType, DecimalType)):
            count_non_zero = df.filter(F.col(colonne) != 0).count()
            if count_non_zero > 0:
                colonnes_a_conserver.append(colonne)
        # Conserver toujours les colonnes de date/heure
        elif isinstance(data_type, TimestampType):
            colonnes_a_conserver.append(colonne)
        # Pour les colonnes de type String, vérifier si elles contiennent des valeurs non vides
        elif isinstance(data_type, StringType):
            count_non_empty = df.filter((F.col(colonne) != "") & (F.col(colonne).isNotNull())).count()
            if count_non_empty > 0:
                colonnes_a_conserver.append(colonne)
        # Conserver les autres types de colonnes
        else:
            colonnes_a_conserver.append(colonne)

    # Retourner un DataFrame ne contenant que les colonnes à conserver
    return df.select(colonnes_a_conserver)

def suplistcol(df, liste_colonnes_a_supprimer):
    """
    Supprime une liste spécifique de colonnes du DataFrame
    
    Args:
        df (DataFrame): DataFrame d'entrée
        liste_colonnes_a_supprimer (list): Liste des colonnes à supprimer
        
    Returns:
        DataFrame: DataFrame sans les colonnes spécifiées
    """
    colonnes_a_conserver = [colonne for colonne in df.columns if colonne not in liste_colonnes_a_supprimer]
    return df.select(colonnes_a_conserver)

def to_silver_parquet(spark_df, output_dirsilver, partition_col="année", filename="Meteo_silver.parquet"):
    """
    Écrit le DataFrame au format Parquet dans le répertoire silver, avec partitionnement
    
    Args:
        spark_df (DataFrame): DataFrame à écrire
        output_dirsilver (str): Répertoire de sortie
        partition_col (str): Colonne de partitionnement (par défaut: "année")
        filename (str): Nom du fichier de sortie (non utilisé directement)
        
    Returns:
        None: Écrit simplement le fichier Parquet
    """
    output_path = os.path.join(output_dirsilver)

    # Ajout de la colonne de partitionnement si nécessaire
    if partition_col == "année" and partition_col not in spark_df.columns:
        spark_df = spark_df.withColumn("année", F.year(spark_df.datetime))
    elif partition_col == "mois" and partition_col not in spark_df.columns:
        spark_df = spark_df.withColumn("mois", F.month(spark_df.datetime))
    elif partition_col == "jour" and partition_col not in spark_df.columns:
        spark_df = spark_df.withColumn("jour", F.dayofmonth(spark_df.datetime))

    # Écriture en mode append avec partitionnement
    spark_df.write.mode("append").partitionBy(partition_col).parquet(output_path)

def to_silver_hive(spark_df, hive_table, database="silver", partition_col=None, mode="append"):
    """
    Stocke un DataFrame Spark dans une table Hive avec partitionnement optionnel.
    
    Args:
        spark_df (DataFrame): Le DataFrame Spark à stocker
        hive_table (str): Nom de la table Hive cible
        database (str): Nom de la base de données (par défaut: "silver")
        partition_col (str): Colonne de partitionnement (optionnel)
        mode (str): Mode d'écriture (par défaut: "append")
    """
    from pyspark.sql import SparkSession
    
    # Créer la base de données si elle n'existe pas
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    
    # Vérifier si un partitionnement est nécessaire et créer la colonne si elle n'existe pas
    if partition_col is not None:
        if partition_col == "année" and partition_col not in spark_df.columns:
            spark_df = spark_df.withColumn("année", F.year(spark_df.datetime))
        elif partition_col == "mois" and partition_col not in spark_df.columns:
            spark_df = spark_df.withColumn("mois", F.month(spark_df.datetime))
        elif partition_col == "jour" and partition_col not in spark_df.columns:
            spark_df = spark_df.withColumn("jour", F.dayofmonth(spark_df.datetime))

    # Construire le nom complet de la table
    full_table_name = f"{database}.{hive_table}"
    
    # Écrire les données dans Hive avec ou sans partitionnement
    if partition_col is not None:
        spark_df.write.mode(mode).partitionBy(partition_col).format("hive").saveAsTable(full_table_name)
        print(f"Données stockées avec succès dans la table Hive {full_table_name}, partitionnées par {partition_col}")
    else:
        spark_df.write.mode(mode).format("hive").saveAsTable(full_table_name)
        print(f"Données stockées avec succès dans la table Hive {full_table_name}")

# Définition des groupes de colonnes pour différentes mesures météorologiques
# Ces listes seront utilisées pour calculer des moyennes par groupe

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

# Début du traitement des données
# 1. Importation des données brutes
print("Importation du fichier Parquet")
Meteo_brute = import_parquet(input_dirbronze, parquet_file)
print("DataFrame créé avec succès")

# 2. Création d'une vue temporaire pour exécuter des requêtes SQL sur les données
print("Création table meteo avec succès")
VueTempSparkSQL(Meteo_brute, "Meteo")
print("Table Meteo créée avec succès")

# 3. Conversion des types de données et ajout de colonnes temporelles
print("debut traitement des types des colonnes avant incertion météo")
meteo_type = convert_datatype(Meteo_brute)
print("fin traitement typages des colonns météo")

# 4. Calcul des moyennes pour chaque groupe de colonnes
# Chaque appel à la fonction moyenne crée une nouvelle colonne contenant la moyenne du groupe
meteo_myenne1 = moyenne(meteo_type, "moyenne_wind_speed", wind_speed)
meteo_myenne2 = moyenne(meteo_myenne1, "moyenne_wind_direction", wind_direction)
meteo_myenne3 = moyenne(meteo_myenne2, "moyenne_air_temperature", air_temperature)
meteo_myenne4 = moyenne(meteo_myenne3, "moyenne_soil_temperature", soil_temperature)
meteo_myenne5 = moyenne(meteo_myenne4, "moyenne_soil_moisture", soil_moisture)

# 5. Suppression des colonnes inutiles
# D'abord, on supprime les colonnes où toutes les valeurs sont 0 ou vides
print("Debut suppression des colonnes")
meteo_filtre = suppcolzero(meteo_myenne5)

# Ensuite, on supprime les colonnes originales qui ont été moyennées
# puisqu'on a maintenant une colonne de moyenne pour chaque groupe
meteo_filtre2 = suplistcol(meteo_filtre, wind_speed)
meteo_filtre3 = suplistcol(meteo_filtre2, wind_direction)
meteo_filtre4 = suplistcol(meteo_filtre3, air_temperature)
meteo_filtre5 = suplistcol(meteo_filtre4, soil_temperature)
meteo_filtre6 = suplistcol(meteo_filtre5, soil_moisture)
print("Fin suppression des colonnes")

# 6. Affichage d'un aperçu du résultat final
print("Affichage des 5 premières lignes du DataFrame")
print(meteo_filtre6.show(5))

# 7. Stockage des résultats
# Écriture au format Parquet dans le répertoire silver
to_silver_parquet(meteo_filtre6, output_dirsilver)

# Stockage dans une table Hive
to_silver_hive(meteo_filtre6, "meteo", database="silver", partition_col="année", mode="append")
print("Données insérées avec succès dans la table Hive et le fichier Parquet")