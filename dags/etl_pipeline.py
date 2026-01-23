from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import random
import json
import plotly.express as px
import webbrowser  # for auto-opening visualization

# ------------------ CONFIG ------------------
DATA_PATH = "/opt/airflow/data"
os.makedirs(DATA_PATH, exist_ok=True)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


# ------------------ GPS SIMULATION ------------------
def extract_gps(**kwargs):
    """
    Generate GPS data for real BMTC routes from bmtc_routes.json
    """
    routes_file = os.path.join(DATA_PATH, "bmtc_routes.json")
    with open(routes_file, "r") as f:
        routes = json.load(f)

    # Simulate at least 6 buses
    bus_ids = [f"BMT{i+101}" for i in range(6)]
    gps_data = []

    for bus in bus_ids:
        route = random.choice(routes)
        stop = random.choice(route["stops"])
        latitude = stop["y"] + random.uniform(-0.001, 0.001)
        longitude = stop["x"] + random.uniform(-0.001, 0.001)

        gps_data.append({
            "bus_id": bus,
            "route": route["route"],
            "next_stop": stop["name"],
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    df = pd.DataFrame(gps_data)
    kwargs["ti"].xcom_push(key="gps_data", value=df.to_dict(orient="records"))
    print("✅ Extracted GPS data:\n", df.head())


# ------------------ VALIDATION ------------------
def validate_data(**kwargs):
    data = kwargs["ti"].xcom_pull(key="gps_data", task_ids="extract_gps")
    df = pd.DataFrame(data)

    if df.isnull().any().any():
        raise ValueError("❌ Null values found in GPS data!")

    if not ((df["latitude"].between(-90, 90)) & (df["longitude"].between(-180, 180))).all():
        raise ValueError("❌ Invalid GPS coordinates!")

    print("✅ Validation passed for", len(df), "records.")


# ------------------ TRANSFORMATION ------------------
def transform(**kwargs):
    data = kwargs["ti"].xcom_pull(key="gps_data", task_ids="extract_gps")
    df = pd.DataFrame(data)
    df["estimated_arrival_min"] = np.random.randint(5, 25, size=len(df))
    transformed_path = os.path.join(DATA_PATH, "transformed_gps.csv")
    df.to_csv(transformed_path, index=False)
    print(f"✅ Transformed and saved to {transformed_path}")


# ------------------ LOAD ------------------
def load_to_csv():
    transformed_path = os.path.join(DATA_PATH, "transformed_gps.csv")
    output_path = os.path.join(DATA_PATH, "bus_locations.csv")

    df = pd.read_csv(transformed_path)

    if os.path.exists(output_path):
        existing = pd.read_csv(output_path)
        all_data = pd.concat([existing, df])
        all_data = all_data[pd.to_datetime(all_data["timestamp"]) >
                            (datetime.now() - timedelta(hours=3))]
    else:
        all_data = df

    all_data.to_csv(output_path, index=False)
    print(f"✅ Loaded data saved to {output_path} ({len(all_data)} rows).")

    log_path = os.path.join(DATA_PATH, "pipeline_log.csv")
    with open(log_path, "a") as f:
        f.write(f"{datetime.now()}, success, {len(df)} records added\n")


# ------------------ VISUALIZATION ------------------
def visualize():
    csv_path = os.path.join(DATA_PATH, "bus_locations.csv")
    df = pd.read_csv(csv_path)

    if df.empty:
        print("⚠️ No data for visualization.")
        return

    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="route",
        hover_name="next_stop",
        zoom=12,
        mapbox_style="carto-positron",
        title=f"BMTC Bus Locations - {datetime.now().strftime('%H:%M:%S')}"
    )
    output_html = os.path.join(DATA_PATH, "bus_map.html")
    fig.write_html(output_html)
    print(f"✅ Visualization updated at {output_html}")

    try:
        webbrowser.open(f"file://{output_html}")
    except Exception as e:
        print(f"⚠️ Could not auto-open visualization: {e}")


# ------------------ DAILY SUMMARY ------------------
def daily_summary():
    path = os.path.join(DATA_PATH, "bus_locations.csv")
    if not os.path.exists(path):
        print("⚠️ No data for summary.")
        return

    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    summary = (
        df.groupby("next_stop")
        .agg(avg_eta=("estimated_arrival_min", "mean"), count=("bus_id", "count"))
        .reset_index()
        .sort_values(by="count", ascending=False)
    )

    summary_path = os.path.join(DATA_PATH, "daily_summary.csv")
    summary.to_csv(summary_path, index=False)
    print(f"📊 Daily summary saved to {summary_path}")


# ------------------ DAG 1: ETL Pipeline ------------------
with DAG(
    dag_id="bmtc_etl_pipeline",
    default_args=default_args,
    description="Enhanced ETL pipeline for BMTC GPS data",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 11, 4),
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract_gps", python_callable=extract_gps, provide_context=True)
    t2 = PythonOperator(task_id="validate_data", python_callable=validate_data, provide_context=True)
    t3 = PythonOperator(task_id="transform", python_callable=transform, provide_context=True)
    t4 = PythonOperator(task_id="load_to_csv", python_callable=load_to_csv)
    t5 = PythonOperator(task_id="visualize", python_callable=visualize)

    t1 >> t2 >> t3 >> t4 >> t5


# ------------------ DAG 2: Daily Summary ------------------
with DAG(
    dag_id="bmtc_daily_summary",
    default_args=default_args,
    description="Daily analytics for BMTC bus data",
    schedule="@daily",
    start_date=datetime(2025, 11, 4),
    catchup=False,
) as summary_dag:

    summary_task = PythonOperator(
        task_id="daily_summary",
        python_callable=daily_summary
    )
