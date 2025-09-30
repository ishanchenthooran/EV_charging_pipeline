import streamlit as st
import pandas as pd
import sqlite3

DB_PATH = "ev_charging.db"

# Load KPIs
def load_kpis():
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT
            s.station_id,
            COUNT(f.session_id) AS total_sessions,
            SUM(f.success) * 1.0 / COUNT(f.session_id) AS success_rate,
            AVG(f.energy_kwh) AS avg_energy_kwh,
            AVG(f.duration_hours) AS avg_duration_hours
        FROM fact_charging f
        JOIN dim_station s ON f.station_id = s.station_id
        GROUP BY s.station_id
        ORDER BY s.station_id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Load time series
def load_time_series():
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df

# ---- Streamlit UI ----
st.set_page_config(page_title="EV Charging Dashboard")

st.title("EV Charging Reliability Dashboard")

tab1, tab2 = st.tabs(["Station KPIs", "Time Series"])

with tab1:
    st.header("Per Station Metrics")
    df_kpis = load_kpis()
    st.dataframe(df_kpis)
    st.bar_chart(df_kpis.set_index("station_id")[["success_rate", "avg_energy_kwh", "avg_duration_hours"]])

with tab2:
    st.header("Daily Averages")
    df_ts = load_time_series()
    st.line_chart(df_ts.set_index("date")[["avg_energy", "avg_duration"]])
