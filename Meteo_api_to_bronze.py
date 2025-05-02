import openmeteo_requests
import requests_cache
import os
from retry_requests import retry
import pyarrow as pa
import pyarrow.parquet as pq
import time 
from datetime import datetime, timedelta

def get_weather(locations, date_start, date_end, hourly_variables=None, output_path=None, output_filename=None):
    
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
    
    # Configuration de la session pour l'API
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    # URL de l'API
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Initialisation des listes pour stocker les données
    all_datetimes = []
    all_location_names = []
    all_latitudes = []
    all_longitudes = []
    all_data = {var: [] for var in hourly_variables}
    
    # Récupération des données pour chaque location
    for location_name, lat, lon in locations:
        print(f"Récupération des données pour {location_name} ({lat}, {lon})...")
        
        # Paramètres de requête API
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date_start,
            "end_date": date_end,
            "hourly": hourly_variables,
            "models": "best_match"
        }
        
        # Appel à l'API
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        
        # Extraction des données horaires
        hourly = response.Hourly()
        
        # Création de la liste des dates/heures
        start_time = datetime.fromtimestamp(hourly.Time())
        end_time = datetime.fromtimestamp(hourly.TimeEnd())
        interval_seconds = hourly.Interval()
        
        # Génération des timestamps
        current_time = start_time
        date_times = []
        while current_time < end_time:
            date_times.append(current_time)
            current_time += timedelta(seconds=interval_seconds)
        
        # Ajout des dates/heures à la liste globale
        all_datetimes.extend(date_times)
        
        # Ajout des informations de location
        all_location_names.extend([location_name] * len(date_times))
        all_latitudes.extend([lat] * len(date_times))
        all_longitudes.extend([lon] * len(date_times))
        
        # Ajout de toutes les variables horaires
        for i, variable in enumerate(hourly_variables):
            values = hourly.Variables(i).ValuesAsNumpy()
            all_data[variable].extend(values)
        
        # Pause pour éviter de surcharger l'API
        if len(locations) > 1 and locations.index((location_name, lat, lon)) < len(locations) - 1:
            print(f"Pause de 60 secondes avant la prochaine requête...")
            time.sleep(60)
    
    # Conversion des listes en arrays PyArrow
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
    
    # Création de la table PyArrow
    table = pa.Table.from_pydict(data_dict)
    
    # Si un chemin de sortie est spécifié, sauvegarder en Parquet
    if output_path:
        # Création du dossier s'il n'existe pas
        os.makedirs(output_path, exist_ok=True)
        
        # Création d'un nom de fichier informatif si non spécifié
        if output_filename is None:
            date_info = f"{date_start}_to_{date_end}".replace("-", "")
            output_filename = f"weather_data_{date_info}.parquet"
        
        # Chemin complet du fichier
        parquet_path = os.path.join(output_path, output_filename)
        
        # Sauvegarde de la table en format Parquet
        pq.write_table(table, parquet_path)
        
        print(f"Données sauvegardées au format Parquet: {parquet_path}")
    
    return {
        "table": table,
        "locations": locations,
        "date_range": (date_start, date_end),
        "num_rows": len(all_datetimes),
        "variables": hourly_variables
    }

us_airports = [ 
    ("Atlanta", 33.64, -84.43),
    ("Denver", 39.86, -104.67),
    ("Los Angeles", 33.94, -118.41)
]

# Chemin de sortie pour les fichiers Parquet'
output_dir = "bronze"

# Importer les données météo depuis l'API
print("debut importation des données météo")
get_weather(us_airports,date_start="2018-01-01", date_end="2020-12-31",hourly_variables=None, output_path=output_dir, output_filename="Meteo.parquet")
# Insérer les données météo dans la base de données bronze
print("Données météo récupérées avec succès")
