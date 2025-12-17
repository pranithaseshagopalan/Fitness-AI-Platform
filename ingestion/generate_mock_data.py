import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
NUM_USERS = 1000
DAYS_OF_DATA = 30

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def random_date(start_date, end_date):
    """Generate random datetime between two datetimes"""
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)

# -----------------------------
# 1. USER PROFILE DATA
# -----------------------------
def generate_user_profiles():
    users = []

    for i in range(NUM_USERS):
        user = {
            "user_id": f"user_{i+1}",
            "age": random.randint(18, 60),
            "gender": random.choice(["male", "female", "other"]),
            "signup_date": (
                datetime.now() - timedelta(days=random.randint(30, 365))
            ).date().isoformat(),
            "plan_type": random.choice(["free", "premium"])
        }
        users.append(user)

    return users

def write_user_profiles(users):
    file_path = RAW_DATA_DIR / "user_profile.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=users[0].keys())
        writer.writeheader()
        writer.writerows(users)

# -----------------------------
# 2. WORKOUT EVENTS (JSON)
# -----------------------------
def generate_workout_events(users):
    events = []

    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_OF_DATA)

    workout_types = ["yoga", "run", "strength", "cycling"]

    for user in users:
        num_workouts = random.randint(5, 40)

        for _ in range(num_workouts):
            start_time = random_date(start_date, end_date)
            duration = random.choice([20, 30, 45, 60])

            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user["user_id"],
                "event_type": "workout_completed",
                "workout_type": random.choice(workout_types),
                "timestamp": start_time.isoformat(),
                "duration_min": duration
            }
            events.append(event)

    return events

def write_workout_events(events):
    file_path = RAW_DATA_DIR / "workout_events.json"
    with open(file_path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")  # JSON Lines format

# -----------------------------
# 3. HEALTH METRICS
# -----------------------------
def generate_health_metrics(users):
    metrics = []

    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_OF_DATA)

    metric_types = ["heart_rate", "steps", "calories"]

    for user in users:
        for _ in range(random.randint(50, 150)):
            metric = {
                "user_id": user["user_id"],
                "metric_type": random.choice(metric_types),
                "value": random.randint(50, 150) if random.random() < 0.5 else random.randint(1000, 10000),
                "recorded_at": random_date(start_date, end_date).isoformat()
            }
            metrics.append(metric)

    return metrics

def write_health_metrics(metrics):
    file_path = RAW_DATA_DIR / "health_metrics.csv"
    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=metrics[0].keys())
        writer.writeheader()
        writer.writerows(metrics)

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    users = generate_user_profiles()
    write_user_profiles(users)

    workout_events = generate_workout_events(users)
    write_workout_events(workout_events)

    health_metrics = generate_health_metrics(users)
    write_health_metrics(health_metrics)

    print("Mock fitness app data generated successfully.")
