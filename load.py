import os
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def save_to_csv(df, filename, output_dir="data", append=False):
    if df is None or df.empty:
        logger.warning(f"No data to save to {filename}")
        return None

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Construct path
    file_path = os.path.join(output_dir, filename)

    if append and os.path.exists(file_path):
        try:
            # Load existing data
            existing_df = pd.read_csv(file_path)

            # Combine with new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)

            # Remove duplicates
            if "stations" in filename.lower():
                if "id" in combined_df.columns:
                    combined_df.drop_duplicates(subset=["id"], keep="last", inplace=True)

            # For utilization data, use timestamp and connector_id
            elif "utilization" in filename.lower():
                if "timestamp" in combined_df.columns and "connector_id" in combined_df.columns:
                    combined_df.drop_duplicates(subset=["timestamp", "connector_id"], keep="last", inplace=True)

            # For hourly data, use hourly_timestamp and station_id
            elif "hourly" in filename.lower():
                if "hourly_timestamp" in combined_df.columns and "station_id" in combined_df.columns:
                    combined_df.drop_duplicates(subset=["hourly_timestamp", "station_id"], keep="last", inplace=True)

            # Save the combined data
            combined_df.to_csv(file_path, index=False)
            logger.info(f"Appended {len(df)} records to {file_path}, total {len(combined_df)} records")

            return file_path

        except Exception as e:
            logger.error(f"Error appending to existing file {file_path}: {e}")
            logger.info("Falling back to overwrite mode")

    # Save to CSV
    df.to_csv(file_path, index=False)
    logger.info(f"Saved {len(df)} records to {file_path}")

    return file_path


def load_data(stations_path=None, utilization_path=None, hourly_path=None):
    stations_df = None
    utilization_df = None
    hourly_df = None

    # Load stations data
    if stations_path and os.path.exists(stations_path):
        try:
            stations_df = pd.read_csv(stations_path)
            logger.info(f"Loaded {len(stations_df)} stations from {stations_path}")
        except Exception as e:
            logger.error(f"Error loading stations data: {e}")

    # Load utilization data
    if utilization_path and os.path.exists(utilization_path):
        try:
            utilization_df = pd.read_csv(utilization_path)

            # Convert timestamp columns to datetime
            for col in ['timestamp', 'hourly_timestamp']:
                if col in utilization_df.columns:
                    utilization_df[col] = pd.to_datetime(utilization_df[col])

            logger.info(f"Loaded {len(utilization_df)} utilization records from {utilization_path}")
        except Exception as e:
            logger.error(f"Error loading utilization data: {e}")

    # Load hourly data
    if hourly_path and os.path.exists(hourly_path):
        try:
            hourly_df = pd.read_csv(hourly_path)

            # Convert timestamp column to datetime
            if 'hourly_timestamp' in hourly_df.columns:
                hourly_df['hourly_timestamp'] = pd.to_datetime(hourly_df['hourly_timestamp'])

            logger.info(f"Loaded {len(hourly_df)} hourly records from {hourly_path}")
        except Exception as e:
            logger.error(f"Error loading hourly data: {e}")

    return stations_df, utilization_df, hourly_df


