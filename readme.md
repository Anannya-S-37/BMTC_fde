This project is an ETL (Extract, Transform, Load) pipeline built using Apache Airflow for processing BMTC bus GPS data.
It simulates live GPS data, validates and transforms it, and then stores the processed information for visualization and analytics.

System Description

The system works as a complete data pipeline for bus tracking. It reads raw data from files, processes it in Airflow, and produces a clean dataset and an interactive map showing bus locations.

Main components:

GPS Simulator: Generates sample bus GPS data using JSON and CSV files.

Airflow DAGs: Orchestrate tasks for extraction, validation, transformation, loading, and visualization.

Storage Layer: Saves processed data as CSV files in /opt/airflow/data or optionally in a Postgres database.

Visualization: Uses Plotly to create an HTML map that shows current bus positions.

Monitoring and Logging: Airflow logs all task runs. Prometheus and Grafana can be added for system metrics.

Data Flow

Extracts bus location data from the simulator or source files.

Validates and filters invalid coordinates or missing fields.

Transforms the data by adding new fields like estimated arrival time.

Loads the data into CSV files or a Postgres database for storage.

Visualizes results using Plotly by generating an HTML map.

Optionally monitors pipeline health and task status.