# PipelineMonitoring

Certainly! Here's a GitHub README file content for your Python script:

---

# Pipeline Monitoring Dashboard

This repository contains a Dash-based web application for monitoring pipelines in a SingleStore database. The dashboard provides real-time insights into data ingestion, file states, pipeline configuration, and ingestion lag.

## Features

- **Real-Time Monitoring**: The dashboard updates every 2 seconds to provide the latest data.
- **Data Ingestion Speed**: Displays the ingestion speed in different units (Rows/sec, KBs/sec, Batches/sec) using a gauge.
- **File State Visualization**: Shows the state of the files ingested by the pipeline in a pie chart.
- **Pipeline Configuration Details**: Displays the configuration details of the selected pipeline.
- **Ingestion Lag**: Shows the current ingestion lag in pipeline cursors.
- **Error Details**: Allows users to view error details for skipped files.

## Prerequisites

Before running the application, ensure you have the following installed:

- Python 3.7+
- Dash
- Pandas
- SQLAlchemy
- psutil
- singlestoredb
- Plotly
- Dash DAQ
- Logging

You can install the required packages using `pip`:

```sh
pip install dash pandas sqlalchemy psutil singlestoredb plotly dash-daq
```

## Setup

1. Clone the repository:

    ```sh
    git clone https://github.com/yourusername/pipeline-monitoring-dashboard.git
    cd pipeline-monitoring-dashboard
    ```

2. Add your SingleStore connection string in the `create_db_connection` function:

    ```python
    def create_db_connection():
        return s2.connect('<ADD CONNECTION STRING HERE>')
    ```

3. Ensure no other process is using the Dash port (default is 8050). The script will automatically kill any process using this port.

## Running the Application

To run the application, execute:

```sh
python pipeline_monitoring_dash.py
```

The dashboard will be available at `http://0.0.0.0:8050`.

## Usage

### Selecting a Database and Pipeline

- Select a database from the "Select Database" dropdown.
- Select a pipeline from the "Select Pipeline" dropdown.

### Viewing Pipeline Details

- The dashboard will display the file states, ingestion speed, and pipeline configuration details for the selected pipeline.
- Error details for skipped files can be viewed by clicking the "Show Error" button next to the file name.

### Ingestion Speed

- The ingestion speed gauge displays the current speed of data ingestion.
- The speed can be viewed in different units by selecting from the "Speed Unit" dropdown.

## Dashboard Layout

- **Header**: Displays the application title and logo.
- **Database and Pipeline Selection**: Dropdowns for selecting the database and pipeline.
- **Real-Time Data Display**: Gauge for data ingestion speed, pie chart for file states, and ingestion lag display.
- **Pipeline Configuration**: Detailed view of the selected pipeline's configuration.
- **File List**: Scrollable list of files with their states and error details.

## Troubleshooting

### Common Issues

- **Database Connection**: Ensure the connection string is correct and the SingleStore database is accessible.
- **Port Conflict**: If another process is using port 8050, the script will attempt to kill it. Ensure this behavior is acceptable in your environment.

### Logs

The application logs are set to the INFO level and will display in the console. Adjust the logging level as needed in the `logging.basicConfig` call.

For any questions or suggestions, please contact [hagarwal@singlestore.com] or [apraveen@singlestore.com].

---

Feel free to customize the README further according to your specific requirements or preferences.
