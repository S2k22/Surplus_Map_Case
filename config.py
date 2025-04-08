import os

# Base directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Create directories if they don't exist
for directory in [DATA_DIR, LOG_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Configuration dictionary
CONFIG = {
    # API settings
    "api": {
        "url": "https://charging.eviny.no/api/map/chargingStations",
        "timeout": 30,  # seconds
        "max_retries": 3,
        "retry_delay": 5,  # seconds
    },

    # Pipeline settings
    "pipeline": {
        "duration_hours": 24,
        "interval_minutes": 60,
        "output_format": "csv",
    },

    # CSV settings
    "csv": {
        "output_dir": DATA_DIR,
        "stations_file": "charging_stations.csv",
        "utilization_file": "utilization_data.csv",
        "hourly_file": "hourly_utilization.csv",
    },

    # Logging settings
    "logging": {
        "level": "INFO",
        "log_file": os.path.join(LOG_DIR, "pipeline.log"),
        "max_size": 10 * 1024 * 1024,  # 10 MB
        "backup_count": 5,
    },
}