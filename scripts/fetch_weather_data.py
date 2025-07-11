# scripts/fetch_weather_data.py

import requests
import pandas as pd
import os
from datetime import datetime
import time
import subprocess
import sys
import json

# Major cities with their coordinates
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

def fetch_today_weather(city_name, city_info, today):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': city_info['lat'],
        'longitude': city_info['lon'],
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'windspeed_10m_max',
            'windgusts_10m_max',
            'winddirection_10m_dominant',
            'sunshine_duration',
            'precipitation_probability_max',
            'uv_index_max'
        ],
        'timezone': city_info['timezone'],
        'start_date': today,
        'end_date': today
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data for {city_name}: {e}")
        return None

def get_country(city_name):
    mapping = {
        'New York': 'USA', 'London': 'UK', 'Tokyo': 'Japan', 'Sydney': 'Australia',
        'Paris': 'France', 'Mumbai': 'India', 'Beijing': 'China', 'São Paulo': 'Brazil',
        'Cairo': 'Egypt', 'Moscow': 'Russia', 'Los Angeles': 'USA', 'Dubai': 'UAE',
        'Singapore': 'Singapore', 'Berlin': 'Germany', 'Toronto': 'Canada',
        'Mexico City': 'Mexico', 'Buenos Aires': 'Argentina', 'Lagos': 'Nigeria',
        'Istanbul': 'Turkey', 'Bangkok': 'Thailand'
    }
    return mapping.get(city_name, 'Unknown')

def main():
    today = datetime.utcnow().strftime('%Y-%m-%d')
    print(f"📆 Fetching weather for: {today}")
    records = []

    for city, info in CITIES.items():
        data = fetch_today_weather(city, info, today)
        if data and 'daily' in data:
            daily = data['daily']
            record = {
                'date': today,
                'city': city,
                'country': get_country(city),
                'latitude': data.get('latitude', ''),
                'longitude': data.get('longitude', ''),
                'timezone': data.get('timezone', ''),
                'temperature_max_c': daily['temperature_2m_max'][0],
                'temperature_min_c': daily['temperature_2m_min'][0],
                'temperature_mean_c': daily['temperature_2m_mean'][0],
                'precipitation_mm': daily['precipitation_sum'][0],
                'windspeed_max_kmh': daily['windspeed_10m_max'][0],
                'windgust_max_kmh': daily['windgusts_10m_max'][0],
                'wind_direction_degrees': daily['winddirection_10m_dominant'][0],
                'sunshine_duration_hours': round(daily['sunshine_duration'][0] / 3600, 2),
                'precipitation_probability_max': daily['precipitation_probability_max'][0],
                'uv_index_max': daily['uv_index_max'][0],
                'data_fetched_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }
            records.append(record)
        time.sleep(1)

    if records:
        df = pd.DataFrame(records)
        filename = "weather_daily.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved today's data to {filename}")

        # Upload to Kaggle
        upload_to_kaggle(filename)
    else:
        print("⚠️ No data collected today.")

def upload_to_kaggle(csv_filename):
    dataset_slug = "global-weather-dataset-daily"
    metadata = {
        "title": "Global Weather Dataset - Daily",
        "id": f"{os.environ['KAGGLE_USERNAME']}/{dataset_slug}",
        "licenses": [{"name": "CC0-1.0"}],
        "resources": [{"path": csv_filename, "description": "Daily global weather data"}]
    }
    with open('dataset-metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)

    print("📤 Uploading to Kaggle...")
    subprocess.run([
        "kaggle", "datasets", "version",
        "-p", ".", "-m", f"Daily update - {datetime.utcnow().strftime('%Y-%m-%d')}",
        "--dir-mode", "zip"
    ], check=False)

if __name__ == "__main__":
    main()
