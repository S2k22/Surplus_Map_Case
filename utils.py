
import logging
import os
import json
from datetime import datetime
from logging.handlers import RotatingFileHandler
from config import CONFIG

def setup_logging(log_level=None):

    if log_level is None:
        log_level = CONFIG['logging']['level']
    
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Create log directory if it doesn't exist
    log_dir = os.path.dirname(CONFIG['logging']['log_file'])
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    # Create file handler with rotation
    file_handler = RotatingFileHandler(
        CONFIG['logging']['log_file'],
        maxBytes=CONFIG['logging']['max_size'],
        backupCount=CONFIG['logging']['backup_count']
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(file_formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    logging.info(f"Logging initialized with level {log_level}")

def save_metadata(data, filename='pipeline_metadata.json'):

    output_dir = CONFIG['csv']['output_dir']
    filepath = os.path.join(output_dir, filename)
    
    # Add timestamp if not present
    if 'timestamp' not in data:
        data['timestamp'] = datetime.now().isoformat()
    
    try:
        # Create directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Write to file
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logging.info(f"Metadata saved to {filepath}")
    except Exception as e:
        logging.error(f"Error saving metadata: {str(e)}")

def validate_station_data(station):

    # Required fields
    required_fields = ['id', 'name', 'status']
    
    # Check required fields
    for field in required_fields:
        if field not in station or station[field] is None:
            return False, f"Missing required field: {field}"
    
    # Validate location if present
    if 'location' in station:
        location = station['location']
        if not isinstance(location, dict):
            return False, "Location must be a dictionary"
        
        if 'latitude' not in location or 'longitude' not in location:
            return False, "Location missing latitude or longitude"
        
        # Basic validation of coordinates
        try:
            lat = float(location['latitude'])
            lng = float(location['longitude'])
            
            if lat < -90 or lat > 90:
                return False, f"Invalid latitude: {lat}"
            
            if lng < -180 or lng > 180:
                return False, f"Invalid longitude: {lng}"
        except (ValueError, TypeError):
            return False, "Invalid coordinates format"
    
    # Validate connectors if present
    if 'connectors' in station:
        connectors = station['connectors']
        if not isinstance(connectors, list):
            return False, "Connectors must be a list"
        
        for i, connector in enumerate(connectors):
            if not isinstance(connector, dict):
                return False, f"Connector {i} must be a dictionary"
            
            if 'id' not in connector:
                return False, f"Connector {i} missing id"
            
            if 'status' not in connector:
                return False, f"Connector {i} missing status"
    
    return True, ""

def calculate_summary_statistics(stations_df, utilization_df):

    stats = {
        'timestamp': datetime.now().isoformat(),
        'stations': {
            'total': len(stations_df) if stations_df is not None else 0,
            'status_counts': {},
            'connector_types': {}
        },
        'utilization': {
            'total_records': len(utilization_df) if utilization_df is not None else 0,
            'status_counts': {},
            'occupancy_rate': None
        }
    }
    
    # Calculate station statistics
    if stations_df is not None and not stations_df.empty:
        if 'status' in stations_df.columns:
            status_counts = stations_df['status'].value_counts().to_dict()
            stats['stations']['status_counts'] = status_counts
        
        # Count connector types
        for connector_type in ['ccs_connectors', 'chademo_connectors', 'type2_connectors']:
            if connector_type in stations_df.columns:
                type_name = connector_type.replace('_connectors', '')
                stats['stations']['connector_types'][type_name] = int(stations_df[connector_type].sum())
    
    # Calculate utilization statistics
    if utilization_df is not None and not utilization_df.empty:
        if 'status' in utilization_df.columns:
            status_counts = utilization_df['status'].value_counts().to_dict()
            stats['utilization']['status_counts'] = status_counts
        
        # Calculate occupancy rate
        if 'is_occupied' in utilization_df.columns:
            total_connectors = len(utilization_df)
            occupied_connectors = utilization_df['is_occupied'].sum()
            if total_connectors > 0:
                stats['utilization']['occupancy_rate'] = occupied_connectors / total_connectors
    
    return stats
