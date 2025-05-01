import json
import mysql.connector

# Load JSON
with open("airports.json") as f:
    data = json.load(f)

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="tonio",
    password="efrei1234",
    database="flights"
)
cursor = conn.cursor()

# Prepare the INSERT query
sql = """
INSERT INTO airports (
    id, gmt, airport_id, iata_code, city_iata_code,
    icao_code, country_iso2, geoname_id,
    latitude, longitude, airport_name,
    country_name, phone_number, timezone
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Insert each record
for row in data["data"]:
    values = (
        row["id"],
        int(row["gmt"]),
        row["airport_id"],
        row["iata_code"],
        row["city_iata_code"],
        row["icao_code"],
        row["country_iso2"],
        row.get("geoname_id"),
        float(row["latitude"]) if row["latitude"] else None,
        float(row["longitude"]) if row["longitude"] else None,
        row["airport_name"],
        row["country_name"],
        row["phone_number"],
        row["timezone"]
    )
    cursor.execute(sql, values)

conn.commit()
cursor.close()
conn.close()
