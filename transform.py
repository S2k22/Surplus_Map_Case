import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def transform_stations_data(stations_data):
    logger.info("Transforming stations data")

    if not stations_data:
        logger.warning("No stations data to transform")
        return pd.DataFrame()

    stations_table = []

    for station in stations_data:
        try:
            # Initialize connector counts
            connector_counts = {'CCS': 0, 'CHAdeMO': 0, 'Type2': 0, 'AC Type 2': 0, 'Other': 0}
            total_connectors = 0
            available_connectors = 0

            # Process connectors
            connectors_list = []


            if 'connectors' in station and station['connectors']:
                connectors_list = station['connectors']

            elif 'connectionTypes' in station:
                for conn_type, conn_list in station['connectionTypes'].items():
                    for connector in conn_list:
                        connector['type'] = conn_type
                        connectors_list.append(connector)

            elif 'connectionsTypes' in station:
                for conn_type, conn_list in station['connectionsTypes'].items():
                    for connector in conn_list:
                        connector['type'] = conn_type
                        connectors_list.append(connector)


            if 'totalConnectors' in station and isinstance(station['totalConnectors'], (int, float)):
                total_connectors = station['totalConnectors']

            # Count connectors by type and count available connectors
            for connector in connectors_list:
                connector_type = connector.get('type')
                if connector_type in connector_counts:
                    connector_counts[connector_type] += 1
                else:
                    connector_counts['Other'] += 1

                # Count available connectors for station status
                connector_status = connector.get('status', '').upper()
                if connector_status == 'AVAILABLE':
                    available_connectors += 1

                total_connectors = total_connectors or (len(connectors_list) if connectors_list else 0)

            # Handle and normalize AC Type 2 as Type2
            connector_counts['Type2'] += connector_counts['AC Type 2']

            # Extract location information
            location = station.get('location', {})
            latitude = location.get('lat') if location else None
            longitude = location.get('lng') if location else None

            # Determine station status based on connector availability
            if total_connectors == 0:
                # Use original status if no connectors are defined
                status = station.get('status')
                if status and isinstance(status, str):
                    status_mapping = {
                        'AVAILABLE': 'Available',
                        'OCCUPIED': 'Occupied',
                        'UNAVAILABLE': 'OutOfOrder',
                        'OUT_OF_ORDER': 'OutOfOrder',
                        'PLANNED': 'Planned',
                        'UNDER_CONSTRUCTION': 'UnderConstruction'
                    }
                    status = status_mapping.get(status.upper(), status)
            else:
                # Determine based on connector availability
                if available_connectors > 0:
                    status = 'Available'
                elif available_connectors == 0 and total_connectors > 0:
                    # If no connectors are available but station has connectors, mark as occupied
                    status = 'Occupied'
                else:
                    # Default to using original status if we couldn't determine from connectors
                    original_status = station.get('status', '')
                    if isinstance(original_status, str) and original_status.upper() in ['UNAVAILABLE', 'OUT_OF_ORDER']:
                        status = 'OutOfOrder'
                    elif isinstance(original_status, str) and original_status.upper() in ['PLANNED']:
                        status = 'Planned'
                    elif isinstance(original_status, str) and original_status.upper() in ['UNDER_CONSTRUCTION']:
                        status = 'UnderConstruction'
                    else:
                        status = 'Occupied'  # Default if we can't determine

            # Create station record
            station_record = {
                'id': station.get('id'),
                'name': station.get('name'),
                'operator': station.get('operator', 'Eviny'),  # Default to Eviny based on API
                'status': status,
                'address': station.get('address'),
                'description': station.get('description'),
                'latitude': latitude,
                'longitude': longitude,
                'total_connectors': total_connectors,
                'ccs_connectors': connector_counts['CCS'],
                'chademo_connectors': connector_counts['CHAdeMO'],
                'type2_connectors': connector_counts['Type2'] + connector_counts['AC Type 2'],
                'other_connectors': connector_counts['Other'],
                'amenities': ', '.join(station.get('amenities', [])) if station.get('amenities') else ''
            }

            stations_table.append(station_record)

        except Exception as e:
            logger.warning(f"Error processing station {station.get('id', 'unknown')}: {str(e)}")
            continue

    # Create DataFrame
    stations_df = pd.DataFrame(stations_table)

    # Data quality checks
    if not stations_df.empty:
        stations_with_no_coords = stations_df[stations_df['latitude'].isna() | stations_df['longitude'].isna()].shape[0]
        stations_with_no_name = stations_df[stations_df['name'].isna()].shape[0]

        if stations_with_no_coords > 0:
            logger.warning(f"{stations_with_no_coords} stations missing coordinates")

        if stations_with_no_name > 0:
            logger.warning(f"{stations_with_no_name} stations missing name")

        # No NaN values in status column
        stations_with_no_status = stations_df[stations_df['status'].isna()].shape[0]
        if stations_with_no_status > 0:
            logger.warning(f"{stations_with_no_status} stations have null status, setting to 'Unknown'")
            stations_df['status'].fillna('Unknown', inplace=True)

    logger.info(f"Transformed {len(stations_df)} stations")
    return stations_df


def transform_utilization_data(stations_data, timestamp):
    logger.info("Transforming utilization data")

    if not stations_data:
        logger.warning("No stations data to transform into utilization data")
        return pd.DataFrame()

    utilization_records = []
    timestamp_str = timestamp.isoformat()
    hourly_timestamp = timestamp.replace(minute=0, second=0, microsecond=0).isoformat()

    for station in stations_data:
        try:
            station_id = station.get('id')

            # Process connectors
            connectors_list = []

            # Use the processed connectors list if available
            if 'connectors' in station and station['connectors']:
                connectors_list = station['connectors']
            # Alternative field names
            elif 'connectionTypes' in station:
                for conn_type, conn_list in station['connectionTypes'].items():
                    for connector in conn_list:
                        connector['type'] = conn_type
                        connectors_list.append(connector)
            elif 'connectionsTypes' in station:
                for conn_type, conn_list in station['connectionsTypes'].items():
                    for connector in conn_list:
                        connector['type'] = conn_type
                        connectors_list.append(connector)

            for connector in connectors_list:
                connector_id = connector.get('id')
                connector_type = connector.get('type')

                # Handle different status field names and formats
                connector_status = connector.get('status')
                if connector_status and isinstance(connector_status, str):
                    # Normalize status to our expected format
                    status_mapping = {
                        'AVAILABLE': 'Available',
                        'OCCUPIED': 'Occupied',
                        'UNAVAILABLE': 'OutOfOrder',
                        'OUT_OF_ORDER': 'OutOfOrder'
                    }
                    connector_status = status_mapping.get(connector_status.upper(), connector_status)

                # Extract power information (could be 'power' or 'effect')
                power = connector.get('power', connector.get('effect'))

                # Create utilization record
                utilization_records.append({
                    'timestamp': timestamp_str,
                    'hourly_timestamp': hourly_timestamp,
                    'station_id': station_id,
                    'connector_id': connector_id,
                    'connector_type': connector_type,
                    'power': power,
                    'status': connector_status,
                    'is_occupied': 1 if connector_status == 'Occupied' else 0,
                    'is_available': 1 if connector_status == 'Available' else 0,
                    'is_out_of_order': 1 if connector_status in ('OutOfOrder', 'UNAVAILABLE') else 0,
                    'tariff': connector.get('tariffDefinition', '')
                })

        except Exception as e:
            logger.warning(f"Error processing utilization for station {station.get('id', 'unknown')}: {str(e)}")
            continue

    # Create DataFrame
    utilization_df = pd.DataFrame(utilization_records)

    # Data quality checks
    if not utilization_df.empty:
        status_counts = utilization_df['status'].value_counts()
        logger.info(f"Utilization status counts: {status_counts.to_dict()}")

    logger.info(f"Transformed {len(utilization_df)} utilization records")
    return utilization_df


def aggregate_hourly_utilization(utilization_df):
    logger.info("Aggregating hourly utilization data")

    if utilization_df.empty:
        logger.warning("No utilization data to aggregate")
        return pd.DataFrame()

    # Group by hourly timestamp and station ID
    hourly_df = utilization_df.groupby(['hourly_timestamp', 'station_id']).agg({
        'is_available': 'sum',
        'is_occupied': 'sum',
        'is_out_of_order': 'sum',
        'connector_id': 'count'
    }).reset_index()

    # Rename and calculate additional metrics
    hourly_df.rename(columns={'connector_id': 'total_connectors'}, inplace=True)
    hourly_df['occupancy_rate'] = hourly_df['is_occupied'] / hourly_df['total_connectors']
    hourly_df['availability_rate'] = hourly_df['is_available'] / hourly_df['total_connectors']

    logger.info(f"Created {len(hourly_df)} hourly aggregated records")
    return hourly_df

