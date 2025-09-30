"""
EV Charging Reliability Pipeline (Skeleton)

This script outlines the basic flow of an ETL-style pipeline
for EV charging session data. Minimal implementation for GitHub skeleton.
"""
from __future__ import annotations

import csv
import os
import random
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from typing import List

def generate_synthetic_data(csv_path: str, n_sessions: int = 1000, n_stations: int = 5, seed: int = 42) -> None:
    """Generate a synthetic EV charging sessions dataset and write it to a CSV.

    Each session contains:
      * session_id – unique identifier
      * station_id – identifier of the charging station
      * start_time – ISO8601 timestamp when the session started
      * end_time – ISO8601 timestamp when the session ended
      * energy_kwh – amount of energy delivered in kWh
      * success – 1 if the session completed successfully, 0 otherwise

    Parameters
    ----------
    csv_path : str
        Path to the CSV file to generate.
    n_sessions : int, optional
        Number of charging sessions to simulate, by default 1 000.
    n_stations : int, optional
        Number of unique charging stations to simulate, by default 5.
    seed : int, optional
        Random seed for reproducibility, by default 42.
    """
    random.seed(seed)
    start_date = datetime(2025, 1, 1)
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            ["session_id", "station_id", "start_time", "end_time", "energy_kwh", "success"]
        )
        for session_id in range(1, n_sessions + 1):
            station_id = random.randint(1, n_stations)
            # start between day 0 and day 30
            start_offset_days = random.randint(0, 29)
            # session start time within the day (0–23h)
            start_offset_hours = random.randint(0, 23)
            start_offset_minutes = random.randint(0, 59)
            start_dt = start_date + timedelta(days=start_offset_days, hours=start_offset_hours, minutes=start_offset_minutes)
            # session duration between 15 min and 4 hours
            duration_minutes = random.randint(15, 4 * 60)
            end_dt = start_dt + timedelta(minutes=duration_minutes)
            # energy delivered in kWh: assume 0.5–50 kWh, correlated with duration
            energy_kwh = round(random.uniform(0.5, 0.8) * (duration_minutes / 60.0) * 10, 2)
            # determine success: 90 % of sessions succeed
            success = 1 if random.random() < 0.9 else 0
            writer.writerow(
                [
                    session_id,
                    station_id,
                    start_dt.isoformat(sep="T", timespec="minutes"),
                    end_dt.isoformat(sep="T", timespec="minutes"),
                    energy_kwh,
                    success,
                ]
      )   

def process_data(csv_path: str) -> pd.DataFrame:
    """Load and clean the raw charging sessions CSV into a DataFrame.


    The function parses timestamps, computes session durations (in hours),
    extracts a date key for joining with a time dimension and returns the
    cleaned DataFrame.


    Parameters
    ----------
    csv_path : str
        Path to the CSV file containing raw charging session data.


    Returns
    -------
    pd.DataFrame
        The cleaned DataFrame.
    """
    df = pd.read_csv(csv_path, parse_dates=["start_time", "end_time"])
    # Compute session duration in hours
    df["duration_hours"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 3600.0
    # Extract date key for dim_time (YYYYMMDD integer)
    df["date_key"] = df["start_time"].dt.strftime("%Y%m%d").astype(int)
    return df

def create_database(db_path: str) -> sqlite3.Connection:
    """Create the SQLite database and return the connection.


    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.


    Returns
    -------
    sqlite3.Connection
        An open connection to the newly created database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create dimension tables
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_station (
            station_id INTEGER PRIMARY KEY,
            station_name TEXT,
            location TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_time (
            time_id INTEGER PRIMARY KEY,
            date TEXT,
            day_of_week TEXT,
            month INTEGER,
            year INTEGER
        )
        """
    )
    # Create fact table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_charging (
            session_id INTEGER PRIMARY KEY,
            station_id INTEGER,
            time_id INTEGER,
            energy_kwh REAL,
            duration_hours REAL,
            success INTEGER,
            FOREIGN KEY (station_id) REFERENCES dim_station(station_id),
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
        )
        """
    )
    conn.commit()
    return conn

def populate_dimensions(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Populate the dimension tables with unique values from the DataFrame.


    Parameters
    ----------
    conn : sqlite3.Connection
        An open SQLite connection.
    df : pd.DataFrame
        The cleaned charging sessions DataFrame.
    """
    cursor = conn.cursor()
    # Populate dim_station
    stations = df["station_id"].unique()
    for station_id in stations:
        # Cast numpy types to built‑in Python int to avoid sqlite datatype mismatch
        sid = int(station_id)
        cursor.execute(
            "INSERT OR IGNORE INTO dim_station (station_id, station_name, location) VALUES (?, ?, ?)",
            (sid, f"Station {sid}", f"Location {sid}"),
        )
    # Populate dim_time
    dates = df[["date_key", "start_time"]].drop_duplicates().sort_values("date_key")
    for date_key, start_time in dates.itertuples(index=False):
        date = start_time.date()
        # Cast date_key to Python int in case it's numpy.int64
        tk = int(date_key)
        cursor.execute(
            "INSERT OR IGNORE INTO dim_time (time_id, date, day_of_week, month, year) VALUES (?, ?, ?, ?, ?)",
            (
                tk,
                date.isoformat(),
                date.strftime("%A"),
                date.month,
                date.year,
            ),
        )
    conn.commit()

def populate_fact(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Populate the fact table with charging session data.


    Parameters
    ----------
    conn : sqlite3.Connection
        An open SQLite connection.
    df : pd.DataFrame
        The cleaned charging sessions DataFrame.
    """
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        cursor.execute(
            """
            INSERT OR REPLACE INTO fact_charging (
                session_id, station_id, time_id, energy_kwh, duration_hours, success
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                int(row.session_id),
                int(row.station_id),
                int(row.date_key),
                float(row.energy_kwh),
                float(row.duration_hours),
                int(row.success),
            ),
        )
    conn.commit()

def compute_kpis(conn: sqlite3.Connection) -> pd.DataFrame:
    """Compute KPIs per station: success rate, avg energy, avg duration."""
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
    df = pd.read_sql_query(query, conn)
    return df

def plot_reliability(df: pd.DataFrame, output_path: str) -> None:
    """Plot station reliability as a bar chart.


    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns station_id and success_rate.
    output_path : str
        Path to save the PNG chart.
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; skipping plot generation.")
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
    """Compute daily average energy and duration across all stations."""
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
    # Paths relative to this script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "charging_sessions.csv")
    db_path = os.path.join(base_dir, "ev_charging.db")
    plot_path = os.path.join(base_dir, "station_reliability.png")


    if not os.path.exists(csv_path):
        print(f"Generating synthetic data at {csv_path}...")
        generate_synthetic_data(csv_path)
        print(f"Synthetic data generated with 1 000 sessions.")
    else:
        print(f"Using existing data at {csv_path}.")


    # Load and process data
    df = process_data(csv_path)
    print(f"Loaded {len(df)} sessions.")


    # Create database and populate tables
    # Remove existing database to avoid schema mismatches from prior runs
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = create_database(db_path)
    populate_dimensions(conn, df)
    populate_fact(conn, df)


    # Compute kpi's
    kpi_df = compute_kpis(conn)
    print("Station reliability:")
    print(kpi_df)
    # Plot reliability
    plot_reliability(kpi_df, plot_path)
    if os.path.exists(plot_path):
        print(f"Plot saved to {plot_path}.")
    conn.close()

if __name__ == "__main__":
    main()
