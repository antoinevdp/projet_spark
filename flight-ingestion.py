import requests, mysql.connector, json

response = requests.get("https://api.aviationstack.com/v1/cities?access_key=8169fc0b517e5cdf325278ae336bede4&country_iso2=FR")
response.json()

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(response.json(), f, ensure_ascii=False, indent=4)

cnx = mysql.connector.connect(
    user="tonio",
    password="efrei1234",
)

cursor = cnx.cursor()data