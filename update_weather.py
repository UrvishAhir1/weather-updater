import os
import requests
import pandas as pd
from datetime import date

# --- CONFIG ---
# LOCATIONS = [
#     {"city": "Mumbai",   "lat": 19.0760, "lon": 72.8777},
#     {"city": "New York", "lat": 40.7128, "lon": -74.0060},
# ]

LOCATIONS = [
    {"city": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"city": "New York", "lat": 40.7128, "lon": -74.0060},
    {"city": "London", "lat": 51.5074, "lon": -0.1278},
    {"city": "Tokyo", "lat": 35.6895, "lon": 139.6917},
]

OUTPUT_CSV = "weather_daily.csv"

# Fetch today's date
today = date.today().isoformat()
rows = []

for loc in LOCATIONS:
    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": loc["lat"],
                "longitude": loc["lon"],
                "current_weather": True,
                "timezone": "Asia/Kolkata"
            }
        )
        response.raise_for_status()
        current = response.json().get("current_weather", {})
        if current:
            rows.append({
                "date": today,
                "city": loc["city"],
                "temperature_c": current["temperature"],
                "wind_speed_kmh": current["windspeed"]
            })
    except Exception as e:
        print(f"Error for {loc['city']}: {e}")

# If we got data, write to CSV
if rows:
    new_df = pd.DataFrame(rows)

    if os.path.exists(OUTPUT_CSV):
        old_df = pd.read_csv(OUTPUT_CSV)
        df = pd.concat([old_df, new_df], ignore_index=True)
        df.drop_duplicates(subset=["date", "city"], inplace=True)
    else:
        df = new_df

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Weather data written to {OUTPUT_CSV}")
else:
    print("⚠️ No weather data fetched. Nothing written.")

# Push to Kaggle
os.system(
    "kaggle datasets version -p . "
    f"-m 'Daily weather update for {today}' --dir-mode zip"
)
