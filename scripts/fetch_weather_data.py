import requests
import pandas as pd
import os
import time
from datetime import datetime
import subprocess

# Cities with coordinates and timezones
CITIES = {
    'New York': {'lat': 40.7128, 'lon': -74.0060, 'timezone': 'America/New_York'},
    'London': {'lat': 51.5074, 'lon': -0.1278, 'timezone': 'Europe/London'},
    'Tokyo': {'lat': 35.6762, 'lon': 139.6503, 'timezone': 'Asia/Tokyo'},
    'Sydney': {'lat': -33.8688, 'lon': 151.2093, 'timezone': 'Australia/Sydney'},
    'Paris': {'lat': 48.8566, 'lon': 2.3522, 'timezone': 'Europe/Paris'},
    'Mumbai': {'lat': 19.0760, 'lon': 72.8777, 'timezone': 'Asia/Kolkata'},
    'Beijing': {'lat': 39.9042, 'lon': 116.4074, 'timezone': 'Asia/Shanghai'},
    'São Paulo': {'lat': -23.5558, 'lon': -46.6396, 'timezone': 'America/Sao_Paulo'},
    'Cairo': {'lat': 30.0444, 'lon': 31.2357, 'timezone': 'Africa/Cairo'},
    'Moscow': {'lat': 55.7558, 'lon': 37.6173, 'timezone': 'Europe/Moscow'},
    'Los Angeles': {'lat': 34.0522, 'lon': -118.2437, 'timezone': 'America/Los_Angeles'},
    'Dubai': {'lat': 25.2048, 'lon': 55.2708, 'timezone': 'Asia/Dubai'},
    'Singapore': {'lat': 1.3521, 'lon': 103.8198, 'timezone': 'Asia/Singapore'},
    'Berlin': {'lat': 52.5200, 'lon': 13.4050, 'timezone': 'Europe/Berlin'},
    'Toronto': {'lat': 43.6532, 'lon': -79.3832, 'timezone': 'America/Toronto'},
    'Mexico City': {'lat': 19.4326, 'lon': -99.1332, 'timezone': 'America/Mexico_City'},
    'Buenos Aires': {'lat': -34.6118, 'lon': -58.3960, 'timezone': 'America/Argentina/Buenos_Aires'},
    'Lagos': {'lat': 6.5244, 'lon': 3.3792, 'timezone': 'Africa/Lagos'},
    'Istanbul': {'lat': 41.0082, 'lon': 28.9784, 'timezone': 'Europe/Istanbul'},
    'Bangkok': {'lat': 13.7563, 'lon': 100.5018, 'timezone': 'Asia/Bangkok'},
}

COUNTRIES = {
    'New York': 'USA', 'London': 'UK', 'Tokyo': 'Japan', 'Sydney': 'Australia',
    'Paris': 'France', 'Mumbai': 'India', 'Beijing': 'China', 'São Paulo': 'Brazil',
    'Cairo': 'Egypt', 'Moscow': 'Russia', 'Los Angeles': 'USA', 'Dubai': 'UAE',
    'Singapore': 'Singapore', 'Berlin': 'Germany', 'Toronto': 'Canada',
    'Mexico City': 'Mexico', 'Buenos Aires': 'Argentina', 'Lagos': 'Nigeria',
    'Istanbul': 'Turkey', 'Bangkok': 'Thailand'
}

def fetch_weather(city, info, today):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": info["lat"],
        "longitude": info["lon"],
        "daily": [
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "windspeed_10m_max", "windgusts_10m_max",
            "winddirection_10m_dominant", "sunshine_duration",
            "precipitation_probability_max", "uv_index_max"
        ],
        "timezone": info["timezone"],
        "start_date": today,
        "end_date": today
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Error fetching data for {city}: {e}")
        return None

def main():
    today = datetime.utcnow().strftime('%Y-%m-%d')
    today_filename = f"weather_{today}.csv"
    all_data = []

    print(f"📆 Fetching weather data for {today}...")

    for city, info in CITIES.items():
        res = fetch_weather(city, info, today)
        if not res or 'daily' not in res:
            continue
        daily = res['daily']
        if not daily['temperature_2m_max']:
            continue  # Skip if daily data is missing

        try:
            row = {
                "date": today,
                "city": city,
                "country": COUNTRIES.get(city, "Unknown"),
                "latitude": res.get("latitude"),
                "longitude": res.get("longitude"),
                "timezone": res.get("timezone"),
                "temperature_max_c": daily["temperature_2m_max"][0],
                "temperature_min_c": daily["temperature_2m_min"][0],
                "temperature_mean_c": daily["temperature_2m_mean"][0],
                "precipitation_mm": daily["precipitation_sum"][0],
                "windspeed_max_kmh": daily["windspeed_10m_max"][0],
                "windgust_max_kmh": daily["windgusts_10m_max"][0],
                "wind_direction_degrees": daily["winddirection_10m_dominant"][0],
                "sunshine_duration_hours": round(daily["sunshine_duration"][0] / 3600, 2) if daily["sunshine_duration"][0] else 0,
                "precipitation_probability_max": daily["precipitation_probability_max"][0],
                "uv_index_max": daily["uv_index_max"][0],
                "data_fetched_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }
            all_data.append(row)
        except Exception as e:
            print(f"⚠️ Error processing {city}: {e}")
        time.sleep(1)

    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(today_filename, index=False)
        print(f"✅ Saved data to {today_filename}")
        upload_to_kaggle(today_filename)
    else:
        print("⚠️ No data fetched.")

def upload_to_kaggle(csv_file):
    print("📤 Uploading CSV to Kaggle...")
    subprocess.run([
        "kaggle", "datasets", "version",
        "--file", csv_file,
        "-m", f"Daily update - {datetime.utcnow().strftime('%Y-%m-%d')}"
    ], check=False)

if __name__ == "__main__":
    main()
