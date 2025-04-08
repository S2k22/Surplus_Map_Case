
import logging
import time
from datetime import datetime, timedelta

from extract import extract_charging_stations
from transform import transform_stations_data, transform_utilization_data, aggregate_hourly_utilization
from load import save_to_csv
from data_validation import validate_and_log_data

logger = logging.getLogger(__name__)


def run_continuous_extraction(duration_hours=24, interval_minutes=60, output_dir="data"):
    logger.info(f"Starting continuous extraction for {duration_hours} hours "
                f"with {interval_minutes} minute intervals")

    # Calculate end time
    end_time = datetime.now() + timedelta(hours=duration_hours)

    # Initialize statistics
    stats = {
        'extraction_count': 0,
        'total_stations': 0,
        'total_utilization_records': 0,
        'validation_issues': 0,
        'errors': 0
    }

    try:
        while datetime.now() < end_time:
            current_time = datetime.now()
            logger.info(f"Extraction #{stats['extraction_count'] + 1} at {current_time.isoformat()}")

            # Extract data
            stations_data = extract_charging_stations()

            if not stations_data:
                logger.error("Extraction failed, will retry in the next interval")
                stats['errors'] += 1

                # Calculate wait time until the next interval
                elapsed_seconds = (datetime.now() - current_time).total_seconds()
                wait_seconds = max(0, interval_minutes * 60 - elapsed_seconds)

                if wait_seconds > 0 and datetime.now() + timedelta(seconds=wait_seconds) < end_time:
                    logger.info(f"Waiting {wait_seconds:.1f} seconds until the next extraction...")
                    time.sleep(wait_seconds)

                continue

            # Transform data
            stations_df = transform_stations_data(stations_data)
            utilization_df = transform_utilization_data(stations_data, current_time)

            # Aggregate hourly data
            hourly_df = aggregate_hourly_utilization(utilization_df)

            # Validate data quality
            is_valid = validate_and_log_data(stations_df, utilization_df, hourly_df)

            # Update statistics and track validation results
            stats['extraction_count'] += 1
            stats['total_stations'] = len(stations_df)
            stats['total_utilization_records'] += len(utilization_df)
            if not is_valid:
                stats['validation_issues'] = stats.get('validation_issues', 0) + 1
                logger.warning("Data validation found issues in extraction #%d", stats['extraction_count'])

            # Quality checks
            available_count = utilization_df['is_available'].sum() if not utilization_df.empty else 0
            occupied_count = utilization_df['is_occupied'].sum() if not utilization_df.empty else 0
            out_of_order_count = utilization_df['is_out_of_order'].sum() if not utilization_df.empty else 0

            logger.info(f"Current status: {available_count} available, "
                        f"{occupied_count} occupied, {out_of_order_count} out of order")

            # Save stations data
            stations_filename = f"charging_stations.csv"
            save_to_csv(stations_df, stations_filename, output_dir)

            # Save utilization data
            utilization_filename = f"utilization_data.csv"
            save_to_csv(utilization_df, utilization_filename, output_dir, append=True)

            # Calculate wait time until the next interval
            elapsed_seconds = (datetime.now() - current_time).total_seconds()
            wait_seconds = max(0, interval_minutes * 60 - elapsed_seconds)

            if wait_seconds > 0 and datetime.now() + timedelta(seconds=wait_seconds) < end_time:
                logger.info(f"Waiting {wait_seconds:.1f} seconds until the next extraction...")
                time.sleep(wait_seconds)

    except KeyboardInterrupt:
        logger.info("Continuous extraction interrupted by user")
    except Exception as e:
        logger.exception(f"Continuous extraction failed: {str(e)}")
        stats['errors'] += 1

    # Log summary statistics
    logger.info(f"Continuous extraction complete. Summary statistics:")
    logger.info(f"Total extractions: {stats['extraction_count']}")
    logger.info(f"Total stations in last extraction: {stats['total_stations']}")
    logger.info(f"Total utilization records collected: {stats['total_utilization_records']}")
    logger.info(f"Extractions with validation issues: {stats['validation_issues']}")
    logger.info(f"Total errors: {stats['errors']}")

    return stats