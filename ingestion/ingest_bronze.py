import pandas as pd
from pathlib import Path
from datetime import datetime

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

if __name__ == "__main__":
    ingest_user_profile()
