import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# GOLD: USER DAILY METRICS
# -----------------------------
def create_user_daily_metrics():
    workouts_file = SILVER_DIR / "workout_events_silver.csv"
    users_file = SILVER_DIR / "user_profile_silver.csv"
    output_file = GOLD_DIR / "user_daily_metrics.csv"

    workouts = pd.read_csv(workouts_file)
    users = pd.read_csv(users_file)

    # Ensure timestamp is datetime
    workouts["timestamp"] = pd.to_datetime(workouts["timestamp"])

    # Create workout date
    workouts["workout_date"] = workouts["timestamp"].dt.date

    # Aggregate metrics per user per day
    daily_metrics = (
        workouts
        .groupby(["user_id", "workout_date"])
        .agg(
            total_workouts=("event_id", "count"),
            total_duration_min=("duration_min", "sum"),
            avg_duration_min=("duration_min", "mean"),
            last_activity=("timestamp", "max")
        )
        .reset_index()
    )

    # Join with user profile
    final_df = daily_metrics.merge(
        users,
        on="user_id",
        how="left"
    )

    final_df.to_csv(output_file, index=False)
    print("Gold layer user daily metrics created")


if __name__ == "__main__":
    create_user_daily_metrics()

