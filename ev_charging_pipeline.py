from __future__ import annotations

import csv
import os
import random
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


def generate_synthetic_data(csv_path: str, n_sessions: int = 1000, n_stations: int = 5, seed: int = 42) -> None:
    # Generate synthetic charging session data and save to CSV
    random.seed(seed)
    start_date = datetime(2025, 1, 1)
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["session_id", "station_id", "start_time", "end_time", "energy_kwh", "success"])
        for session_id in range(1, n_sessions + 1):
            station_id = random.randint(1, n_stations)

            # random start time
            start_dt = start_date + timedelta(
                days=random.randint(0, 29),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )

            # duration 15 min – 4 hrs
            duration_minutes = random.randint(15, 4 * 60)
            end_dt = start_dt + timedelta(minutes=duration_minutes)

            # energy roughly proportional to duration
            energy_kwh = round(random.uniform(0.5, 0.8) * (duration_minutes / 60.0) * 10, 2)

            # ~90% sessions succeed
            success = 1 if random.random() < 0.9 else 0

            writer.writerow([
                session_id,
                station_id,
                start_dt.isoformat(sep="T", timespec="minutes"),
                end_dt.isoformat(sep="T", timespec="minutes"),
                energy_kwh,
                success,
            ])


def process_data(csv_path: str) -> pd.DataFrame:
    # Load CSV, add session duration and date key
    df = pd.read_csv(csv_path, parse_dates=["start_time", "end_time"])
    df["duration_hours"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 3600.0
    df["date_key"] = df["start_time"].dt.strftime("%Y%m%d").astype(int)
    return df


def create_database(db_path: str) -> sqlite3.Connection:
    # Create SQLite DB with dimension and fact tables
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS dim_station (
            station_id INTEGER PRIMARY KEY,
            station_name TEXT,
            location TEXT
        )"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS dim_time (
            time_id INTEGER PRIMARY KEY,
            date TEXT,
            day_of_week TEXT,
            month INTEGER,
            year INTEGER
        )"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS fact_charging (
            session_id INTEGER PRIMARY KEY,
            station_id INTEGER,
            time_id INTEGER,
            energy_kwh REAL,
            duration_hours REAL,
            success INTEGER,
            FOREIGN KEY (station_id) REFERENCES dim_station(station_id),
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
        )"""
    )
    conn.commit()
    return conn


def populate_dimensions(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    # Insert unique stations and dates into dimension tables
    cursor = conn.cursor()

    for station_id in df["station_id"].unique():
        cursor.execute(
            "INSERT OR IGNORE INTO dim_station (station_id, station_name, location) VALUES (?, ?, ?)",
            (int(station_id), f"Station {station_id}", f"Location {station_id}"),
        )

    for date_key, start_time in df[["date_key", "start_time"]].drop_duplicates().itertuples(index=False):
        date = start_time.date()
        cursor.execute(
            "INSERT OR IGNORE INTO dim_time (time_id, date, day_of_week, month, year) VALUES (?, ?, ?, ?, ?)",
            (int(date_key), date.isoformat(), date.strftime("%A"), date.month, date.year),
        )

    conn.commit()


def populate_fact(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    # Insert session records into fact table
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        cursor.execute(
            """INSERT OR REPLACE INTO fact_charging
               (session_id, station_id, time_id, energy_kwh, duration_hours, success)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (int(row.session_id), int(row.station_id), int(row.date_key),
             float(row.energy_kwh), float(row.duration_hours), int(row.success)),
        )
    conn.commit()


def compute_kpis(conn: sqlite3.Connection) -> pd.DataFrame:
    # Return KPIs per station (success rate, avg energy, avg duration)
    query = """
        SELECT
            station_id,
            COUNT(*) AS total_sessions,
            SUM(success) * 1.0 / COUNT(*) AS success_rate,
            AVG(energy_kwh) AS avg_energy_kwh,
            AVG(duration_hours) AS avg_duration_hours
        FROM fact_charging
        GROUP BY station_id
        ORDER BY station_id
    """
    return pd.read_sql_query(query, conn)


def plot_reliability(df: pd.DataFrame, output_path: str) -> None:
    # Plot success rate per station as bar chart
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; skipping plot.")
        return

    plt.figure(figsize=(8, 4))
    plt.bar(df["station_id"].astype(str), df["success_rate"])
    plt.xlabel("Station ID")
    plt.ylabel("Success Rate")
    plt.title("EV Charging Station Reliability")
    plt.ylim(0, 1)

    for i, rate in enumerate(df["success_rate"]):
        plt.text(i, rate + 0.02, f"{rate:.2f}", ha="center")

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def compute_time_series(conn: sqlite3.Connection) -> pd.DataFrame:
    # Return daily averages of energy and duration
    query = """
        SELECT
            t.date,
            AVG(f.energy_kwh) AS avg_energy,
            AVG(f.duration_hours) AS avg_duration
        FROM fact_charging f
        JOIN dim_time t ON f.time_id = t.time_id
        GROUP BY t.date
        ORDER BY t.date
    """
    df = pd.read_sql_query(query, conn)
    df["date"] = pd.to_datetime(df["date"])
    return df


def main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "charging_sessions.csv")
    db_path = os.path.join(base_dir, "ev_charging.db")
    plot_path = os.path.join(base_dir, "station_reliability.png")

    # create synthetic data if not present
    if not os.path.exists(csv_path):
        print(f"Generating synthetic data at {csv_path}...")
        generate_synthetic_data(csv_path)
    else:
        print(f"Using existing data at {csv_path}.")

    # load and clean
    df = process_data(csv_path)
    print(f"Loaded {len(df)} sessions.")

    # reset DB
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = create_database(db_path)

    populate_dimensions(conn, df)
    populate_fact(conn, df)

    # KPIs
    kpi_df = compute_kpis(conn)
    print("Station KPIs:")
    print(kpi_df)

    # chart
    plot_reliability(kpi_df, plot_path)
    if os.path.exists(plot_path):
        print(f"Plot saved to {plot_path}.")

    conn.close()


if __name__ == "__main__":
    main()
