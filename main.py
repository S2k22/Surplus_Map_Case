import os
import argparse
import logging
from datetime import datetime

# Import the pipeline modules
from extract import extract_charging_stations
from transform import transform_stations_data, transform_utilization_data, aggregate_hourly_utilization
from load import save_to_csv
from data_validation import validate_and_log_data
from config import CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_single_extraction(output_dir=None):

    if output_dir is None:
        output_dir = CONFIG['csv']['output_dir']

    logger.info("Running single extraction")

    # Extract data
    stations_data = extract_charging_stations()

    if not stations_data:
        logger.error("Extraction failed")
        return None, None, None

    # Transform stations data
    stations_df = transform_stations_data(stations_data)

    # Transform utilization data
    current_time = datetime.now()
    utilization_df = transform_utilization_data(stations_data, current_time)

    # Aggregate hourly data
    hourly_df = aggregate_hourly_utilization(utilization_df)

    # Validate data quality
    is_valid = validate_and_log_data(stations_df, utilization_df, hourly_df)
    if not is_valid:
        logger.warning("Data validation found issues, but continuing with save operation")

    # Save to CSV
    save_to_csv(stations_df, CONFIG['csv']['stations_file'], output_dir)
    save_to_csv(utilization_df, CONFIG['csv']['utilization_file'], output_dir)
    save_to_csv(hourly_df, CONFIG['csv']['hourly_file'], output_dir)

    logger.info("Single extraction completed successfully")
    return stations_df, utilization_df, hourly_df


def run_continuous_extraction(duration_hours=24, interval_minutes=60, output_dir=None):

    import time
    from datetime import timedelta

    if output_dir is None:
        output_dir = CONFIG['csv']['output_dir']

    logger.info(f"Starting continuous extraction for {duration_hours} hours with {interval_minutes} minute intervals")

    # Calculate end time
    end_time = datetime.now() + timedelta(hours=duration_hours)

    # Initialize tracking variables
    extraction_count = 0
    success_count = 0

    try:
        while datetime.now() < end_time:
            extraction_count += 1
            logger.info(f"Extraction #{extraction_count} at {datetime.now().isoformat()}")

            # Run a single extraction
            stations_df, utilization_df, hourly_df = run_single_extraction(output_dir)

            if stations_df is not None:
                success_count += 1

            # Calculate time to next extraction
            next_time = datetime.now() + timedelta(minutes=interval_minutes)

            # Sleep until next extraction time if not past end time
            if next_time < end_time:
                sleep_seconds = (next_time - datetime.now()).total_seconds()
                if sleep_seconds > 0:
                    logger.info(f"Sleeping for {sleep_seconds:.1f} seconds until next extraction")
                    time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        logger.info("Continuous extraction interrupted by user")
    except Exception as e:
        logger.error(f"Error during continuous extraction: {e}")
        return False

    logger.info(f"Continuous extraction completed: {success_count}/{extraction_count} successful extractions")
    return True


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="EV Charging Stations ETL pipeline")
    parser.add_argument("--single", action="store_true", help="Run a single extraction")
    parser.add_argument("--duration", type=int, default=CONFIG['pipeline']['duration_hours'],
                        help="Duration for continuous extraction (hours)")
    parser.add_argument("--interval", type=int, default=CONFIG['pipeline']['interval_minutes'],
                        help="Interval between extractions (minutes)")
    parser.add_argument("--output-dir", type=str, default=CONFIG['csv']['output_dir'],
                        help="Output directory for data files")
    parser.add_argument("--visualize", action="store_true",
                        help="Create visualizations after extraction")

    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    if args.single:
        # Run a single extraction
        stations_df, utilization_df, hourly_df = run_single_extraction(args.output_dir)

        # Create visualizations if requested
        if args.visualize and stations_df is not None:
            try:
                from visualize import create_map_visualization, create_utilization_visualizations

                # Create visualization directory
                vis_dir = os.path.join(args.output_dir, "visualizations")
                os.makedirs(vis_dir, exist_ok=True)

                # Generate visualizations
                logger.info("Generating visualizations...")
                map_path = create_map_visualization(stations_df, vis_dir)
                if map_path:
                    logger.info(f"Map visualization created: {map_path}")

                util_paths = create_utilization_visualizations(stations_df, utilization_df, hourly_df, vis_dir)
                if util_paths:
                    logger.info(f"Created {len(util_paths)} utilization visualizations")
            except Exception as e:
                logger.error(f"Error creating visualizations: {e}")
    else:
        # Run continuous extraction
        run_continuous_extraction(args.duration, args.interval, args.output_dir)

        # Create visualizations if requested
        if args.visualize:
            try:
                from visualize import load_data, create_map_visualization, create_utilization_visualizations

                # Load the latest data
                logger.info("Loading data for visualizations...")
                stations_df, utilization_df, hourly_df = load_data(args.output_dir)

                if stations_df is not None:
                    # Create visualization directory
                    vis_dir = os.path.join(args.output_dir, "visualizations")
                    os.makedirs(vis_dir, exist_ok=True)

                    # Generate visualizations
                    logger.info("Generating visualizations...")
                    map_path = create_map_visualization(stations_df, vis_dir)
                    if map_path:
                        logger.info(f"Map visualization created: {map_path}")

                    util_paths = create_utilization_visualizations(stations_df, utilization_df, hourly_df, vis_dir)
                    if util_paths:
                        logger.info(f"Created {len(util_paths)} utilization visualizations")
                else:
                    logger.error("No data available for visualizations")
            except Exception as e:
                logger.error(f"Error creating visualizations: {e}")


if __name__ == "__main__":
    main()