import requests
import pandas as pd
from datetime import datetime


def extract_aqi_data(city, token, timeout=10):
    """Fetch air quality data for a given city using the AQICN API."""
    url = f"https://api.waqi.info/feed/{city}/?token={token}"
    response = requests.get(url, timeout=timeout)

    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")

    data = response.json()

    if data.get("status") != "ok":
        raise Exception(f"API status not ok for city {city}: {data}")

    result = data["data"]
    iaqi = result.get("iaqi", {})

    extracted = {
        "city": result["city"]["name"],
        "timestamp": result["time"]["s"],
        "aqi": result.get("aqi", None),
        "pm25": iaqi.get("pm25", {}).get("v"),
        "pm10": iaqi.get("pm10", {}).get("v"),
        "temperature": iaqi.get("t", {}).get("v"),
        "humidity": iaqi.get("h", {}).get("v")
    }

    df = pd.DataFrame([extracted])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df
