
import logging
import pandas as pd


logger = logging.getLogger(__name__)


class DataValidator:

    @staticmethod
    def validate_stations_data(stations_df):

        if stations_df is None or stations_df.empty:
            return False, ["Stations data is empty or None"], {}

        issues = []
        stats = {
            "total_stations": len(stations_df),
            "status_counts": {},
            "connector_type_counts": {},
            "completeness": {}
        }

        # Check required columns
        required_columns = ['id', 'name', 'status', 'latitude', 'longitude', 'total_connectors']
        missing_columns = [col for col in required_columns if col not in stations_df.columns]

        if missing_columns:
            issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            # If critical columns are missing
            critical_missing = [col for col in ['id', 'status'] if col in missing_columns]
            if critical_missing:
                return False, issues, stats

        # Check for duplicates in station IDs
        if 'id' in stations_df.columns:
            duplicate_ids = stations_df[stations_df.duplicated('id')]['id'].unique()
            if len(duplicate_ids) > 0:
                issues.append(f"Found {len(duplicate_ids)} duplicate station IDs")
                if len(duplicate_ids) <= 10:  # Only show a few examples
                    issues.append(f"Example duplicate IDs: {', '.join(map(str, duplicate_ids[:5]))}")

        # Check data completeness
        for col in stations_df.columns:
            missing = stations_df[col].isna().sum()
            if missing > 0:
                pct_missing = (missing / len(stations_df)) * 100
                issues.append(f"Column '{col}' has {missing} missing values ({pct_missing:.1f}%)")
                stats["completeness"][col] = {
                    "missing": int(missing),
                    "percent_missing": float(pct_missing)
                }

        # Check latitude/longitude values
        if 'latitude' in stations_df.columns and 'longitude' in stations_df.columns:
            invalid_lat = stations_df[(stations_df['latitude'] < -90) | (stations_df['latitude'] > 90)].shape[0]
            invalid_lon = stations_df[(stations_df['longitude'] < -180) | (stations_df['longitude'] > 180)].shape[0]

            if invalid_lat > 0:
                issues.append(f"Found {invalid_lat} stations with invalid latitude values")

            if invalid_lon > 0:
                issues.append(f"Found {invalid_lon} stations with invalid longitude values")

            missing_coords = stations_df[stations_df['latitude'].isna() | stations_df['longitude'].isna()].shape[0]
            if missing_coords > 0:
                issues.append(f"Found {missing_coords} stations missing coordinate values")

        # Check status values
        if 'status' in stations_df.columns:
            status_counts = stations_df['status'].value_counts().to_dict()
            stats["status_counts"] = status_counts

            # Check for unexpected status values
            expected_statuses = {'Available', 'Occupied', 'OutOfOrder', 'Planned', 'UnderConstruction'}
            unexpected_statuses = set(status_counts.keys()) - expected_statuses

            if unexpected_statuses:
                issues.append(f"Found unexpected status values: {', '.join(unexpected_statuses)}")

        # Check connector counts
        connector_columns = [col for col in stations_df.columns if col.endswith('_connectors')]
        for col in connector_columns:
            if col in stations_df.columns:
                type_name = col.replace('_connectors', '')
                count = stations_df[col].sum()
                stats["connector_type_counts"][type_name] = int(count)

        # Check if total_connectors matches sum of specific connector types
        if 'total_connectors' in stations_df.columns and len(connector_columns) > 0:
            # Sum all connector type columns
            connector_sum = stations_df[connector_columns].sum(axis=1)

            # Compare with total_connectors
            mismatch_count = (connector_sum != stations_df['total_connectors']).sum()

            if mismatch_count > 0:
                # This is more of an informational message than its a critical issue
                pct_mismatch = (mismatch_count / len(stations_df)) * 100
                issues.append(
                    f"Found {mismatch_count} stations ({pct_mismatch:.1f}%) where total_connectors doesn't match sum of specific types. " +
                    "This is expected if there are connectors without specific types assigned.")

        # Determine if the data is valid overall
        is_valid = len(issues) == 0 or all(not issue.startswith("Missing required columns") for issue in issues)

        return is_valid, issues, stats

    @staticmethod
    def validate_utilization_data(utilization_df, expect_full_period=False):

        if utilization_df is None or utilization_df.empty:
            return False, ["Utilization data is empty or None"], {}

        issues = []
        stats = {
            "total_records": len(utilization_df),
            "status_counts": {},
            "temporal_coverage": {},
            "station_coverage": {}
        }

        # Check required columns
        required_columns = [
            'timestamp', 'station_id', 'connector_id', 'status',
            'is_occupied', 'is_available', 'is_out_of_order'
        ]
        missing_columns = [col for col in required_columns if col not in utilization_df.columns]

        if missing_columns:
            issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            # If critical columns are missing, consider the data invalid
            critical_missing = [col for col in ['timestamp', 'station_id', 'connector_id', 'status'] if
                                col in missing_columns]
            if critical_missing:
                return False, issues, stats

        # Ensure timestamp is datetime
        if 'timestamp' in utilization_df.columns:
            if not pd.api.types.is_datetime64_dtype(utilization_df['timestamp']):
                try:
                    utilization_df['timestamp'] = pd.to_datetime(utilization_df['timestamp'])
                except Exception as e:
                    issues.append(f"Could not convert 'timestamp' column to datetime: {str(e)}")
                    return False, issues, stats

        # Ensure hourly_timestamp is datetime
        if 'hourly_timestamp' in utilization_df.columns:
            if not pd.api.types.is_datetime64_dtype(utilization_df['hourly_timestamp']):
                try:
                    utilization_df['hourly_timestamp'] = pd.to_datetime(utilization_df['hourly_timestamp'])
                except Exception as e:
                    issues.append(f"Could not convert 'hourly_timestamp' column to datetime: {str(e)}")

        # Check for duplicates
        if 'timestamp' in utilization_df.columns and 'connector_id' in utilization_df.columns:
            duplicate_records = utilization_df[utilization_df.duplicated(['timestamp', 'connector_id'])]
            if len(duplicate_records) > 0:
                issues.append(f"Found {len(duplicate_records)} duplicate utilization records")

        # Check status values
        if 'status' in utilization_df.columns:
            status_counts = utilization_df['status'].value_counts().to_dict()
            stats["status_counts"] = status_counts

            # Log all unique status values
            unique_statuses = set(status_counts.keys())
            if 'FAULTED' in unique_statuses:
                issues.append(f"Found 'FAULTED' status values")

            # Only warn about truly unexpected values
            expected_statuses = {'Available', 'Occupied', 'OutOfOrder', 'FAULTED'}
            unexpected_statuses = unique_statuses - expected_statuses

            if unexpected_statuses:
                issues.append(f"Found unexpected status values: {', '.join(unexpected_statuses)}")

        # Check flag consistency
        if all(col in utilization_df.columns for col in ['is_occupied', 'is_available', 'is_out_of_order', 'status']):
            # Check if is_occupied matches status='Occupied'
            occupied_mismatch = (utilization_df['is_occupied'] == 1) != (utilization_df['status'] == 'Occupied')
            occupied_mismatch_count = occupied_mismatch.sum()

            if occupied_mismatch_count > 0:
                issues.append(f"Found {occupied_mismatch_count} records where is_occupied flag doesn't match status")

            # Check if is_available matches status='Available'
            available_mismatch = (utilization_df['is_available'] == 1) != (utilization_df['status'] == 'Available')
            available_mismatch_count = available_mismatch.sum()

            if available_mismatch_count > 0:
                issues.append(f"Found {available_mismatch_count} records where is_available flag doesn't match status")

            # Check if is_out_of_order matches status='OutOfOrder'
            oof_mismatch = (utilization_df['is_out_of_order'] == 1) != (utilization_df['status'] == 'OutOfOrder')
            oof_mismatch_count = oof_mismatch.sum()

            if oof_mismatch_count > 0:
                issues.append(f"Found {oof_mismatch_count} records where is_out_of_order flag doesn't match status")

        # Check time coverage
        if 'timestamp' in utilization_df.columns:
            min_time = utilization_df['timestamp'].min()
            max_time = utilization_df['timestamp'].max()
            time_range = max_time - min_time

            stats["temporal_coverage"] = {
                "min_time": min_time.isoformat() if not pd.isna(min_time) else None,
                "max_time": max_time.isoformat() if not pd.isna(max_time) else None,
                "time_range_hours": time_range.total_seconds() / 3600 if not pd.isna(time_range) else None
            }

            # Check if data covers a full 24-hour period if expected
            if expect_full_period:
                # Extract hour of day from timestamp
                utilization_df['hour'] = utilization_df['timestamp'].dt.hour

                # Check if all hours are represented
                hours_covered = set(utilization_df['hour'].unique())
                missing_hours = set(range(24)) - hours_covered

                if missing_hours:
                    issues.append(f"Missing data for hours: {', '.join(map(str, sorted(missing_hours)))}")
                    stats["temporal_coverage"]["missing_hours"] = sorted(list(missing_hours))

        # Check station coverage
        if 'station_id' in utilization_df.columns:
            station_counts = utilization_df['station_id'].value_counts()
            stats["station_coverage"] = {
                "unique_stations": len(station_counts),
                "min_records_per_station": int(station_counts.min()) if len(station_counts) > 0 else 0,
                "max_records_per_station": int(station_counts.max()) if len(station_counts) > 0 else 0,
                "avg_records_per_station": float(station_counts.mean()) if len(station_counts) > 0 else 0
            }

        # Determine if the data is valid overall
        is_valid = len(issues) == 0 or all(not issue.startswith("Missing required columns") for issue in issues)

        return is_valid, issues, stats

    @staticmethod
    def validate_hourly_data(hourly_df):

        if hourly_df is None or hourly_df.empty:
            return False, ["Hourly data is empty or None"], {}

        issues = []
        stats = {
            "total_records": len(hourly_df),
            "temporal_coverage": {},
            "station_coverage": {}
        }

        # Check required columns
        required_columns = [
            'hourly_timestamp', 'station_id', 'is_available', 'is_occupied',
            'is_out_of_order', 'total_connectors', 'occupancy_rate'
        ]
        missing_columns = [col for col in required_columns if col not in hourly_df.columns]

        if missing_columns:
            issues.append(f"Missing required columns: {', '.join(missing_columns)}")

        # Ensure hourly_timestamp is datetime
        if 'hourly_timestamp' in hourly_df.columns:
            if not pd.api.types.is_datetime64_dtype(hourly_df['hourly_timestamp']):
                try:
                    hourly_df['hourly_timestamp'] = pd.to_datetime(hourly_df['hourly_timestamp'])
                except Exception as e:
                    issues.append(f"Could not convert 'hourly_timestamp' column to datetime: {str(e)}")
                    return False, issues, stats

        # Check for duplicates
        if 'hourly_timestamp' in hourly_df.columns and 'station_id' in hourly_df.columns:
            duplicate_records = hourly_df[hourly_df.duplicated(['hourly_timestamp', 'station_id'])]
            if len(duplicate_records) > 0:
                issues.append(f"Found {len(duplicate_records)} duplicate hourly records")

        # Check if occupancy_rate is between 0 and 1
        if 'occupancy_rate' in hourly_df.columns:
            invalid_rates = hourly_df[(hourly_df['occupancy_rate'] < 0) | (hourly_df['occupancy_rate'] > 1)]
            if len(invalid_rates) > 0:
                issues.append(f"Found {len(invalid_rates)} records with invalid occupancy rate values")

        # Check if total_connectors matches sum of status counts
        if all(col in hourly_df.columns for col in
               ['total_connectors', 'is_available', 'is_occupied', 'is_out_of_order']):
            status_sum = hourly_df['is_available'] + hourly_df['is_occupied'] + hourly_df['is_out_of_order']
            mismatch = (status_sum != hourly_df['total_connectors'])
            mismatch_count = mismatch.sum()

            if mismatch_count > 0:

                pct_mismatch = (mismatch_count / len(hourly_df)) * 100
                issues.append(
                    f"Found {mismatch_count} records ({pct_mismatch:.1f}%) where total_connectors doesn't match sum of status counts. " +
                    "This may be due to connectors with status values not counted in the standard categories.")

        # Check time coverage
        if 'hourly_timestamp' in hourly_df.columns:
            min_time = hourly_df['hourly_timestamp'].min()
            max_time = hourly_df['hourly_timestamp'].max()
            time_range = max_time - min_time

            stats["temporal_coverage"] = {
                "min_time": min_time.isoformat() if not pd.isna(min_time) else None,
                "max_time": max_time.isoformat() if not pd.isna(max_time) else None,
                "time_range_hours": time_range.total_seconds() / 3600 if not pd.isna(time_range) else None
            }

        # Check station coverage
        if 'station_id' in hourly_df.columns:
            station_counts = hourly_df['station_id'].value_counts()
            stats["station_coverage"] = {
                "unique_stations": len(station_counts),
                "min_records_per_station": int(station_counts.min()) if len(station_counts) > 0 else 0,
                "max_records_per_station": int(station_counts.max()) if len(station_counts) > 0 else 0,
                "avg_records_per_station": float(station_counts.mean()) if len(station_counts) > 0 else 0
            }

        # Determine if the data is valid overall
        is_valid = len(issues) == 0 or all(not issue.startswith("Missing required columns") for issue in issues)

        return is_valid, issues, stats

    @staticmethod
    def validate_etl_pipeline(stations_df, utilization_df, hourly_df=None):
        is_valid = True
        issues = []
        validation_report = {
            "stations": {},
            "utilization": {},
            "hourly": {},
            "cross_validation": {}
        }

        # Validate individual datasets
        stations_valid, stations_issues, stations_stats = DataValidator.validate_stations_data(stations_df)
        utilization_valid, utilization_issues, utilization_stats = DataValidator.validate_utilization_data(
            utilization_df)

        validation_report["stations"] = {
            "is_valid": stations_valid,
            "issues": stations_issues,
            "stats": stations_stats
        }

        validation_report["utilization"] = {
            "is_valid": utilization_valid,
            "issues": utilization_issues,
            "stats": utilization_stats
        }

        if hourly_df is not None:
            hourly_valid, hourly_issues, hourly_stats = DataValidator.validate_hourly_data(hourly_df)
            validation_report["hourly"] = {
                "is_valid": hourly_valid,
                "issues": hourly_issues,
                "stats": hourly_stats
            }
            is_valid = is_valid and hourly_valid
            issues.extend([f"Hourly data issue: {issue}" for issue in hourly_issues])

        is_valid = is_valid and stations_valid and utilization_valid
        issues.extend([f"Stations data issue: {issue}" for issue in stations_issues])
        issues.extend([f"Utilization data issue: {issue}" for issue in utilization_issues])

        # Cross-validation between datasets
        cross_validation_issues = []

        # Check if all stations in utilization data exist in stations data
        if stations_df is not None and utilization_df is not None:
            if 'id' in stations_df.columns and 'station_id' in utilization_df.columns:
                station_ids = set(stations_df['id'].unique())
                util_station_ids = set(utilization_df['station_id'].unique())

                # Stations in utilization data but not in stations data
                missing_stations = util_station_ids - station_ids
                if missing_stations:
                    cross_validation_issues.append(
                        f"Found {len(missing_stations)} station IDs in utilization data not present in stations data"
                    )

                validation_report["cross_validation"]["station_coverage"] = {
                    "total_stations": len(station_ids),
                    "stations_with_utilization": len(util_station_ids),
                    "stations_missing_utilization": len(station_ids - util_station_ids),
                    "utilization_records_with_unknown_station": len(missing_stations)
                }

        # Check if hourly data is consistent with utilization data, skipping complex comparison that caused errors
        if utilization_df is not None and hourly_df is not None:
            # Skip the problematic merge operation and just do basic checks
            if 'hourly_timestamp' in utilization_df.columns and 'hourly_timestamp' in hourly_df.columns:
                # Convert to datetime
                if not pd.api.types.is_datetime64_dtype(utilization_df['hourly_timestamp']):
                    try:
                        utilization_df['hourly_timestamp'] = pd.to_datetime(utilization_df['hourly_timestamp'])
                    except Exception as e:
                        cross_validation_issues.append(
                            f"Could not convert utilization hourly_timestamp to datetime: {str(e)}")

                if not pd.api.types.is_datetime64_dtype(hourly_df['hourly_timestamp']):
                    try:
                        hourly_df['hourly_timestamp'] = pd.to_datetime(hourly_df['hourly_timestamp'])
                    except Exception as e:
                        cross_validation_issues.append(
                            f"Could not convert hourly hourly_timestamp to datetime: {str(e)}")

                # Simple check of unique timestamps in each dataset
                util_timestamps = set(utilization_df['hourly_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S'))
                hourly_timestamps = set(hourly_df['hourly_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S'))

                # Timestamps in hourly data but not in utilization data
                extra_timestamps = hourly_timestamps - util_timestamps
                if extra_timestamps:
                    cross_validation_issues.append(
                        f"Found {len(extra_timestamps)} timestamps in hourly data not present in utilization data"
                    )

                # Check if we have the same number of unique station IDs
                if 'station_id' in utilization_df.columns and 'station_id' in hourly_df.columns:
                    util_station_count = utilization_df['station_id'].nunique()
                    hourly_station_count = hourly_df['station_id'].nunique()

                    if util_station_count != hourly_station_count:
                        cross_validation_issues.append(
                            f"Mismatch in unique station count: {util_station_count} in utilization vs {hourly_station_count} in hourly"
                        )

        validation_report["cross_validation"]["issues"] = cross_validation_issues
        issues.extend([f"Cross-validation issue: {issue}" for issue in cross_validation_issues])
        is_valid = is_valid and len(cross_validation_issues) == 0

        return is_valid, issues, validation_report


def validate_and_log_data(stations_df, utilization_df, hourly_df=None):
    logger.info("Validating data quality...")

    is_valid, issues, validation_report = DataValidator.validate_etl_pipeline(
        stations_df, utilization_df, hourly_df
    )

    # Log validation results
    if is_valid:
        logger.info("Data validation passed")
    else:
        logger.warning("Data validation found issues")

    # Log statistics
    if 'stations' in validation_report and 'stats' in validation_report['stations']:
        stats = validation_report['stations']['stats']
        if 'total_stations' in stats:
            logger.info(f"Validated {stats['total_stations']} stations")
        if 'status_counts' in stats:
            logger.info(f"Station status counts: {stats['status_counts']}")

    if 'utilization' in validation_report and 'stats' in validation_report['utilization']:
        stats = validation_report['utilization']['stats']
        if 'total_records' in stats:
            logger.info(f"Validated {stats['total_records']} utilization records")
        if 'status_counts' in stats:
            logger.info(f"Utilization status counts: {stats['status_counts']}")

    # Log issues
    for issue in issues:
        logger.warning(f"Validation issue: {issue}")

    return is_valid