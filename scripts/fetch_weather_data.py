import requests
import pandas as pd
import time
import subprocess
from datetime import datetime

# Cities with coordinates and timezones
CITIES = {
    "New York": {"lat": 40.7128, "lon": -74.0060, "timezone": "America/New_York"},
    "London": {"lat": 51.5074, "lon": -0.1278, "timezone": "Europe/London"},
    "Tokyo": {"lat": 35.6762, "lon": 139.6503, "timezone": "Asia/Tokyo"},
    "Delhi": {"lat": 28.6139, "lon": 77.2090, "timezone": "Asia/Kolkata"},
    "Paris": {"lat": 48.8566, "lon": 2.3522, "timezone": "Europe/Paris"},
    "Sydney": {"lat": -33.8688, "lon": 151.2093, "timezone": "Australia/Sydney"},
    "Moscow": {"lat": 55.7558, "lon": 37.6173, "timezone": "Europe/Moscow"},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777, "timezone": "Asia/Kolkata"},
    "Beijing": {"lat": 39.9042, "lon": 116.4074, "timezone": "Asia/Shanghai"},
    "São Paulo": {"lat": -23.5505, "lon": -46.6333, "timezone": "America/Sao_Paulo"},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437, "timezone": "America/Los_Angeles"},
    "Cairo": {"lat": 30.0444, "lon": 31.2357, "timezone": "Africa/Cairo"},
    "Istanbul": {"lat": 41.0082, "lon": 28.9784, "timezone": "Europe/Istanbul"},
    "Mexico City": {"lat": 19.4326, "lon": -99.1332, "timezone": "America/Mexico_City"},
    "Seoul": {"lat": 37.5665, "lon": 126.9780, "timezone": "Asia/Seoul"},
    "Berlin": {"lat": 52.5200, "lon": 13.4050, "timezone": "Europe/Berlin"},
    "Bangkok": {"lat": 13.7563, "lon": 100.5018, "timezone": "Asia/Bangkok"},
    "Lagos": {"lat": 6.5244, "lon": 3.3792, "timezone": "Africa/Lagos"},
    "Buenos Aires": {"lat": -34.6037, "lon": -58.3816, "timezone": "America/Argentina/Buenos_Aires"},
    "Singapore": {"lat": 1.3521, "lon": 103.8198, "timezone": "Asia/Singapore"},
    "Toronto": {"lat": 43.651070, "lon": -79.347015, "timezone": "America/Toronto"},
}

COUNTRIES = {
    "New York": "USA", "London": "UK", "Tokyo": "Japan", "Delhi": "India", "Paris": "France", "Sydney": "Australia",
    "Moscow": "Russia", "Mumbai": "India", "Beijing": "China", "São Paulo": "Brazil", "Los Angeles": "USA",
    "Cairo": "Egypt", "Istanbul": "Turkey", "Mexico City": "Mexico", "Seoul": "South Korea", "Berlin": "Germany",
    "Bangkok": "Thailand", "Lagos": "Nigeria", "Buenos Aires": "Argentina", "Singapore": "Singapore",
    "Toronto": "Canada"
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

    for attempt in range(1, 4):  # Retry 3 times max
        try:
            r = requests.get(url, params=params, timeout=25)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"❌ Error fetching {city} (attempt {attempt}/3): {e}")
            time.sleep(3)  # Delay before retry
    return None

def main():
    today = datetime.utcnow().strftime('%Y-%m-%d')
    today_filename = f"weather_{today}.csv"
    all_data = []

    print(f"📆 Fetching weather data for {today}...")

    for city, info in CITIES.items():
        res = fetch_weather(city, info, today)
        if not res or 'daily' not in res or not res['daily']['temperature_2m_max']:
            continue
        daily = res['daily']
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
        time.sleep(2)  # Delay between each city to avoid rate-limits

    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(today_filename, index=False)
        print(f"✅ Saved data to {today_filename}")
        upload_to_kaggle(today_filename)
        print(f"📊 Stored rows: {len(all_data)}")
    else:
        print("⚠️ No data fetched.")

def upload_to_kaggle(csv_file):
    print("📤 Uploading to Kaggle…")

    result = subprocess.run([
        "kaggle", "datasets", "version",
        "-p", ".",  # Use current directory
        "-m", f"Daily update - {datetime.utcnow().strftime('%Y-%m-%d')}"
    ], capture_output=True, text=True)

    print("📤 Kaggle CLI stdout:\n", result.stdout)
    if result.stderr:
        print("❌ Kaggle CLI stderr:\n", result.stderr)

if __name__ == "__main__":
    main()
