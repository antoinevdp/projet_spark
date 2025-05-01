import json
import mysql.connector

# Load JSON
with open("data2.json") as f:
    json_data = json.load(f)

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="tonio",
    password="efrei1234",
    database="flights"
)
cursor = conn.cursor()

# Prepare insert statement
sql = """
INSERT INTO cities (id, gmt, city_id, iata_code, country_iso2, geoname_id, latitude, longitude, city_name, timezone)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Insert each record
for record in json_data["data"]:
    values = (
        record["id"],
        record["gmt"],
        record["city_id"],
        record["iata_code"],
        record["country_iso2"],
        record.get("geoname_id"),
        record["latitude"],
        record["longitude"],
        record["city_name"],
        record["timezone"]
    )
    cursor.execute(sql, values)

conn.commit()
cursor.close()
conn.close()
