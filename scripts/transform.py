import os
import pandas as pd

def transform_aqi_data(df: pd.DataFrame, output_path: str = "data/cleaned_air_quality.csv") -> pd.DataFrame:
    """Clean and validate the extracted AQI data."""

    # Drop rows where AQI or timestamp is missing
    df = df.dropna(subset=["aqi", "timestamp"])

    # Normalize city name (capitalize)
    df["city"] = df["city"].str.title()

    # Round numeric columns to 2 decimal places
    for col in ["aqi", "pm25", "pm10", "temperature", "humidity"]:
        df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Replace negative values (if any) with NaN
    for col in ["pm25", "pm10", "temperature", "humidity"]:
        df[col] = df[col].apply(lambda x: x if x is None or x >= 0 else None)

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_csv(output_path, index=False)
    print(f"Transformed data saved to {output_path}")
    return df
