import openmeteo_requests
import requests_cache
import os
import sys
from retry_requests import retry
import pyarrow as pa
import pyarrow.parquet as pq
import time 
from datetime import datetime, timedelta

# Liste des emplacements à surveiller (aéroports US et points intermédiaires)
us_airports = [
    # Principaux aéroports
    ("Atlanta", 33.64, -84.43),              # ATL: Aéroport international Hartsfield-Jackson
    ("Denver", 39.86, -104.67),              # DEN: Aéroport international de Denver
    ("Los Angeles", 33.94, -118.41),         # LAX: Aéroport international de Los Angeles
    ("Chicago O'Hare", 41.978, -87.904),     # ORD: Aéroport international O'Hare de Chicago
    
    # Points intermédiaires sur la route aérienne Denver-Los Angeles
    ("DEN-LAX Point 1", 37.9064, -109.2042),
    ("DEN-LAX Point 2", 35.9528, -113.7384),
    
    # Points intermédiaires sur la route aérienne Los Angeles-Chicago
    ("LAX-ORD Point 1", 36.5925, -108.3430),
    ("LAX-ORD Point 2", 39.2451, -98.2760),
    
    # Points intermédiaires sur la route aérienne Atlanta-Chicago
    ("ATL-ORD Point 1", 36.3915, -85.5764),
    ("ATL-ORD Point 2", 39.1431, -86.7228),
    
    # Points intermédiaires sur la route aérienne Denver-Chicago
    ("DEN-ORD Point 1", 40.5589, -99.1372),
    ("DEN-ORD Point 2", 41.2579, -93.6044),
    
    # Points intermédiaires sur la route aérienne Denver-Atlanta
    ("DEN-ATL Point 1", 37.8074, -97.9908),
    ("DEN-ATL Point 2", 35.7548, -91.3116),
    
    # Points intermédiaires sur la route aérienne Atlanta-Los Angeles
    ("ATL-LAX Point 1", 33.7390, -95.6434),
    ("ATL-LAX Point 2", 33.8380, -106.8568)
]


# Chemin de sortie pour les fichiers Parquet (dossier "bronze" pour architecture en couches)
output_dir = "bronze" 

def get_weather(
    locations,          # Liste de tuples (nom_lieu, latitude, longitude)
    date_start,         # Date de début au format "YYYY-MM-DD"
    date_end,           # Date de fin au format "YYYY-MM-DD"
    hourly_variables=None,  # Variables météo à récupérer (liste par défaut si None)
    output_path=None,   # Chemin du dossier de sortie pour le fichier Parquet
    output_filename=None,  # Nom du fichier de sortie (généré automatiquement si None)
    partition_col=None  # Colonne de partitionnement ("année", "mois", "jour", "heure")
):
    """
    Récupère des données météorologiques historiques via l'API Open-Meteo
    et les enregistre au format Parquet.
    
    Parameters:
    -----------
    locations : list of tuples
        Liste de tuples (nom_lieu, latitude, longitude) pour chaque emplacement
    date_start : str
        Date de début au format "YYYY-MM-DD"
    date_end : str
        Date de fin au format "YYYY-MM-DD"
    hourly_variables : list, optional
        Liste des variables météorologiques à récupérer
    output_path : str, optional
        Chemin du dossier de sortie pour le fichier Parquet
    output_filename : str, optional
        Nom du fichier de sortie (généré automatiquement si None)
    partition_col : str, optional
        Colonne de partitionnement ("année", "mois", "jour", "heure")
        
    Returns:
    --------
    dict
        Dictionnaire contenant la table PyArrow, les emplacements, les dates, 
        le nombre de lignes et les variables récupérées
    """
    
    # Importations nécessaires
    import os
    import time
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import requests_cache
    import openmeteo_requests
    from retry_requests import retry
    from datetime import datetime, timedelta
    
    # Variables horaires par défaut si non spécifiées
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
    
    # Configuration de la session pour l'API avec mise en cache et gestion des réessais
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)  # Cache permanent
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)  # 5 tentatives avec délai exponentiel
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    # URL de l'API Open-Meteo pour les données historiques
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Initialisation des listes pour stocker les données de tous les emplacements
    all_datetimes = []      # Liste pour stocker toutes les dates/heures
    all_location_names = [] # Liste pour stocker tous les noms d'emplacements
    all_latitudes = []      # Liste pour stocker toutes les latitudes
    all_longitudes = []     # Liste pour stocker toutes les longitudes
    all_data = {var: [] for var in hourly_variables}  # Dictionnaire pour stocker toutes les variables météo
    
    # Boucle sur chaque emplacement pour récupérer les données
    for location_name, lat, lon in locations:
        print(f"Récupération des données pour {location_name} ({lat}, {lon})...")
        
        # Paramètres de la requête API
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date_start,
            "end_date": date_end,
            "hourly": hourly_variables,
            "models": "best_match"  # Utilise le meilleur modèle météo disponible
        }
        
        # Appel à l'API Open-Meteo
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]  # Première (et unique) réponse
        
        # Extraction des données horaires de la réponse
        hourly = response.Hourly()
        
        # Extraction des informations temporelles
        start_time = datetime.fromtimestamp(hourly.Time())       # Timestamp de début
        end_time = datetime.fromtimestamp(hourly.TimeEnd())      # Timestamp de fin
        interval_seconds = hourly.Interval()                     # Intervalle en secondes (3600 pour horaire)
        
        # Génération de tous les timestamps pour cet emplacement
        current_time = start_time
        date_times = []
        while current_time < end_time:
            date_times.append(current_time)
            current_time += timedelta(seconds=interval_seconds)  # Ajoute l'intervalle (généralement 1h)
        
        # Ajout des dates/heures à la liste globale
        all_datetimes.extend(date_times)
        
        # Ajout des informations d'emplacement (répétées pour chaque timestamp)
        all_location_names.extend([location_name] * len(date_times))
        all_latitudes.extend([lat] * len(date_times))
        all_longitudes.extend([lon] * len(date_times))
        
        # Extraction et stockage de toutes les variables météo pour cet emplacement
        for i, variable in enumerate(hourly_variables):
            values = hourly.Variables(i).ValuesAsNumpy()  # Récupère les valeurs sous forme d'array numpy
            all_data[variable].extend(values)             # Ajoute les valeurs à la liste globale
        
        # Pause entre les requêtes pour éviter de surcharger l'API (seulement si plusieurs emplacements)
        if len(locations) > 1 and locations.index((location_name, lat, lon)) < len(locations) - 1:
            print(f"Pause de 60 secondes avant la prochaine requête...")
            time.sleep(60)  # Pause d'une minute
    
    # Conversion des listes en arrays PyArrow pour création du fichier Parquet
    datetime_array = pa.array([dt.timestamp() for dt in all_datetimes], type=pa.timestamp('s'))
    location_name_array = pa.array(all_location_names)
    latitude_array = pa.array(all_latitudes)
    longitude_array = pa.array(all_longitudes)
    
    # Création du dictionnaire des colonnes pour la table PyArrow
    data_dict = {
        "datetime": datetime_array,
        "location_name": location_name_array,
        "latitude": latitude_array,
        "longitude": longitude_array
    }
    
    # Ajout des variables météo au dictionnaire
    for variable in hourly_variables:
        data_dict[variable] = pa.array(all_data[variable])
    
    # Création de la table PyArrow avec toutes les données
    table = pa.Table.from_pydict(data_dict)
    
    # Si un chemin de sortie est spécifié, sauvegarde en format Parquet
    if output_path:
        # Création du dossier de sortie s'il n'existe pas
        os.makedirs(output_path, exist_ok=True)
        
        # Génération d'un nom de fichier informatif si non spécifié
        if output_filename is None:
            date_info = f"{date_start}_to_{date_end}".replace("-", "")
            output_filename = f"weather_data_{date_info}.parquet"
        
        # Chemin complet du fichier de sortie
        parquet_path = os.path.join(output_path, output_filename)

        # Traitement du partitionnement si spécifié
        if partition_col is not None:
            # Conversion des timestamps en dates lisibles pour faciliter le partitionnement
            datetimes = pd.Series(pd.to_datetime(table["datetime"].to_pandas(), unit='s'))
            
            # Création de partitions selon la période demandée
            if partition_col == "année":
                # Extraction des années
                partition_values = datetimes.dt.year
                partition_name = "année"
                
            elif partition_col == "mois":
                # Extraction des mois
                partition_values = datetimes.dt.month
                partition_name = "mois"
                
            elif partition_col == "jour":
                # Extraction des jours
                partition_values = datetimes.dt.day
                partition_name = "jour"
                
            elif partition_col == "heure":
                # Extraction des heures
                partition_values = datetimes.dt.hour
                partition_name = "heure"
            
            # Partitionnement pour chaque valeur unique
            unique_values = set(partition_values)
            for value in unique_values:
                # Création d'un masque booléen
                mask = partition_values == value
                # Conversion en array PyArrow
                arrow_mask = pa.array(mask)
                # Filtrage de la table avec le masque
                filtered_table = table.filter(arrow_mask)
                # Création du dossier de partitionnement
                partition_dir = os.path.join(output_path, f"{partition_name}={value}")
                os.makedirs(partition_dir, exist_ok=True)
                # Chemin du fichier dans la partition
                partition_file = os.path.join(partition_dir, output_filename)
                # Écriture du fichier parquet
                pq.write_table(filtered_table, partition_file)
                print(f"Partition pour {partition_name}={value} sauvegardée: {partition_file}")
        else:
            # Sans partitionnement, sauvegarde de la table complète en format Parquet
            pq.write_table(table, parquet_path)
            print(f"Données sauvegardées au format Parquet: {parquet_path}")
    
    # Retourne un dictionnaire contenant les informations sur les données récupérées
    return {
        "table": table,                     # Table PyArrow complète
        "locations": locations,             # Liste des emplacements traités
        "date_range": (date_start, date_end),  # Période couverte
        "num_rows": len(all_datetimes),     # Nombre total de lignes
        "variables": hourly_variables       # Variables météo récupérées
    }

# Exécution de la fonction pour récupérer les données météo sur 3 ans (2018-2020)
print("debut importation des données météo")
get_weather(us_airports, date_start="2018-01-01",  date_end="2020-12-31",hourly_variables=None,output_path=output_dir, output_filename="Meteo.parquet", partition_col="année")
# Message de confirmation
print("Données météo récupérées avec succès")