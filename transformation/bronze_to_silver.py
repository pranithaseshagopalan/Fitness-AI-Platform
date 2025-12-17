import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# WORKOUT EVENTS: BRONZE → SILVER
# -----------------------------
def bronze_to_silver_workout_events():
    input_file = BRONZE_DIR / "workout_events_bronze.csv"
    output_file = SILVER_DIR / "workout_events_silver.csv"

    df = pd.read_csv(input_file)

    # Parse timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Drop invalid timestamps
    df = df.dropna(subset=["timestamp"])

    # Enforce data types
    df["duration_min"] = pd.to_numeric(df["duration_min"], errors="coerce")

    # Remove invalid durations
    df = df[df["duration_min"] > 0]

    # Remove duplicates
    df = df.drop_duplicates(subset=["event_id"])

    # Derived feature
    df["workout_hour"] = df["timestamp"].dt.hour

    df.to_csv(output_file, index=False)
    print("Workout events promoted to Silver layer")


if __name__ == "__main__":
    bronze_to_silver_workout_events()
