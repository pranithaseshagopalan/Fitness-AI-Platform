import pandas as pd
from pathlib import Path
from datetime import datetime
import json

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# BRONZE INGESTION: USER PROFILE
# -----------------------------
def ingest_user_profile():
    input_file = RAW_DIR / "user_profile.csv"
    output_file = BRONZE_DIR / "user_profile_bronze.csv"

    df = pd.read_csv(input_file)

    # Basic schema validation
    required_columns = {
        "user_id",
        "age",
        "gender",
        "signup_date",
        "plan_type"
    }

    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    # Add ingestion timestamp
    df["ingested_at"] = datetime.utcnow().isoformat()

    df.to_csv(output_file, index=False)
    print("User profile ingested to Bronze layer")

#import json

# -----------------------------
# BRONZE INGESTION: WORKOUT EVENTS
# -----------------------------
def ingest_workout_events():
    input_file = RAW_DIR / "workout_events.json"
    output_file = BRONZE_DIR / "workout_events_bronze.csv"

    records = []

    with open(input_file, "r") as f:
        for line in f:
            event = json.loads(line)
            records.append(event)

    df = pd.DataFrame(records)

    required_columns = {
        "event_id",
        "user_id",
        "event_type",
        "workout_type",
        "timestamp",
        "duration_min"
    }

    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns in workout events: {missing_cols}")

    df["ingested_at"] = datetime.utcnow().isoformat()

    df.to_csv(output_file, index=False)
    print("Workout events ingested to Bronze layer")


if __name__ == "__main__":
    ingest_user_profile()
    ingest_workout_events()
