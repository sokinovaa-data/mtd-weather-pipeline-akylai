import json
import requests
from pathlib import Path
HEADERS = {
    "User-Agent": "(mtd-weather-pipeline, email)",
    "Accept": "application/geo+json",
}
LAT, LON = 40.1164, -88.2434

# Step 1: which stations are near me?
r = requests.get(f"https://api.weather.gov/points/{LAT},{LON}", headers=HEADERS, timeout=30)
print("Points status:", r.status_code)
r.raise_for_status()
stations_url = r.json()["properties"]["observationStations"]

r = requests.get(stations_url, headers=HEADERS, timeout=30)
r.raise_for_status()
stations = r.json()["features"]
print("Nearby stations:")
for s in stations[:5]:
    print(" ", s["properties"]["stationIdentifier"], "-", s["properties"]["name"])

# Step 2: latest observation from the first (nearest) station
station_id = stations[0]["properties"]["stationIdentifier"]
r = requests.get(
    f"https://api.weather.gov/stations/{station_id}/observations/latest",
    headers=HEADERS, timeout=30,
)
print("\nObservation status:", r.status_code)
r.raise_for_status()
print(f"Size: {len(r.content) / 1024:.1f} KB")

data = r.json()
p = data["properties"]
print("Station:", station_id)
print("Observed at:", p.get("timestamp"))
print("Conditions:", p.get("textDescription"))
print("Temperature:", p.get("temperature"))
print("Precipitation last hour:", p.get("precipitationLastHour"))
print("Wind speed:", p.get("windSpeed"))

out = Path(__file__).resolve().parent.parent / "sample_data" / "weather_sample.json"
with open(out, "w") as f:
    json.dump(data, f, indent=2)
print("Saved to", out)