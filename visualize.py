import os
import logging
import argparse
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime


# Configuration Defaults

DEFAULT_DATA_DIR = "data"
STATIONS_FILE = "charging_stations.csv"
HOURLY_FILE = "utilization_data.csv"  # CSV containing hourly_timestamp and is_occupied columns
OUTPUT_DIR = os.path.join("data", "visualizations")



# Data Loading Functions
def load_stations_data(data_dir):
    stations_path = os.path.join(data_dir, STATIONS_FILE)
    if os.path.exists(stations_path):
        df = pd.read_csv(stations_path)
        logging.info(f"Loaded {len(df)} station records from {stations_path}")
        return df
    else:
        logging.error(f"Stations file not found: {stations_path}")
        return None


def load_hourly_data(data_dir):
    hourly_path = os.path.join(data_dir, HOURLY_FILE)
    if os.path.exists(hourly_path):
        df = pd.read_csv(hourly_path)
        logging.info(f"Loaded {len(df)} hourly records from {hourly_path}")
        # Ensure hourly_timestamp is parsed as datetime
        if "hourly_timestamp" in df.columns:
            df["hourly_timestamp"] = pd.to_datetime(df["hourly_timestamp"], errors="coerce")
        return df
    else:
        logging.error(f"Hourly data file not found: {hourly_path}")
        return None


def create_map_visualization(stations_df, output_dir=None):
    if output_dir is None:
        output_dir = os.path.join(CONFIG['csv']['output_dir'], "visualizations")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if stations_df is None or len(stations_df) == 0:
        logging.warning("No station data available for map visualization")
        return None

    logging.info("Creating map visualization")

    # Filter out stations without coordinates
    map_data = stations_df.dropna(subset=['latitude', 'longitude'])

    if len(map_data) == 0:
        logging.warning("No stations with valid coordinates for map visualization")
        return None

    # Calculate center point (average of all coordinates)
    center_lat = map_data['latitude'].mean()
    center_lon = map_data['longitude'].mean()

    # Create a map centered at the average coordinates
    ev_map = folium.Map(location=[center_lat, center_lon], zoom_start=6)

    # Add a marker cluster
    marker_cluster = MarkerCluster().add_to(ev_map)

    # Status color mapping
    status_colors = {
        'Available': 'green',
        'Occupied': 'orange',
        'OutOfOrder': 'red',
        'Planned': 'gray',
        'UnderConstruction': 'blue'
    }

    # Add markers for each station
    for _, station in map_data.iterrows():
        # Determine marker color based on status
        status = station.get('status', 'Unknown')
        color = status_colors.get(status, 'black')

        # Create popup content
        popup_content = f"""
        <b>{station['name']}</b><br>
        Status: {status}<br>
        Connectors: {station['total_connectors']}<br>
        """

        # Add connector type info if available
        for connector_type in ['ccs_connectors', 'chademo_connectors', 'type2_connectors']:
            if connector_type in station and station[connector_type] > 0:
                type_name = connector_type.replace('_connectors', '').upper()
                popup_content += f"{type_name}: {station[connector_type]}<br>"

        if 'amenities' in station and pd.notna(station['amenities']) and station['amenities']:
            popup_content += f"Amenities: {station['amenities']}<br>"

        # Add marker to the cluster
        folium.Marker(
            location=[station['latitude'], station['longitude']],
            popup=folium.Popup(popup_content, max_width=300),
            icon=folium.Icon(color=color, icon='bolt', prefix='fa')
        ).add_to(marker_cluster)

    # Add legend
    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey; border-radius: 5px">
    <p><strong>Station Status</strong></p>
    <p><i class="fa fa-circle" style="color:green"></i> Available</p>
    <p><i class="fa fa-circle" style="color:orange"></i> Occupied</p>
    <p><i class="fa fa-circle" style="color:red"></i> Out of Order</p>
    <p><i class="fa fa-circle" style="color:gray"></i> Planned</p>
    <p><i class="fa fa-circle" style="color:blue"></i> Under Construction</p>
    </div>
    """
    ev_map.get_root().html.add_child(folium.Element(legend_html))

    # Save map to HTML file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    map_filename = f"charging_stations_map_{timestamp}.html"
    map_path = os.path.join(output_dir, map_filename)
    ev_map.save(map_path)

    logging.info(f"Map visualization saved to {map_path}")
    return map_path


def create_connector_type_distribution(stations_df, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define the expected connector columns
    connector_columns = ['ccs_connectors', 'chademo_connectors', 'type2_connectors']
    available_columns = [col for col in connector_columns if col in stations_df.columns]
    if not available_columns:
        logging.warning("No connector columns available for pie chart")
        return None

    # Sum each connector type across all stations
    connector_data = {}
    for col in available_columns:
        type_name = col.replace('_connectors', '').upper()
        connector_data[type_name] = stations_df[col].sum()

    if sum(connector_data.values()) == 0:
        logging.warning("No connector data found (all counts are zero)")
        return None

    plt.figure(figsize=(8, 8))
    plt.pie(connector_data.values(), labels=connector_data.keys(), autopct='%1.1f%%',
            startangle=90, colors=sns.color_palette("Set2", len(connector_data)))
    plt.axis('equal')
    plt.title('Connector Type Distribution', fontsize=14)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pie_filename = f"connector_type_distribution_{timestamp}.png"
    pie_path = os.path.join(output_dir, pie_filename)
    plt.savefig(pie_path)
    plt.close()
    logging.info(f"Connector type distribution pie chart saved to {pie_path}")
    return pie_path


def create_busiest_hours_chart(hourly_df, output_dir):

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if hourly_df is None or hourly_df.empty:
        logging.warning("No hourly data available for busiest hours chart")
        return None

    if "hourly_timestamp" not in hourly_df.columns:
        logging.warning("Column 'hourly_timestamp' not found in hourly data")
        return None

    # Drop rows with invalid timestamps
    hourly_df = hourly_df.dropna(subset=["hourly_timestamp"])
    # Extract the hour from the timestamp
    hourly_df["hour"] = hourly_df["hourly_timestamp"].dt.hour

    # Use 'is_occupied' column for occupancy count
    usage_col = "is_occupied"
    if usage_col not in hourly_df.columns:
        logging.warning(f"Column '{usage_col}' not found in hourly data")
        return None

    # Group by hour and sum the usage/occupancy
    hour_usage = hourly_df.groupby("hour")[usage_col].sum().reset_index()
    hour_usage.sort_values("hour", inplace=True)

    plt.figure(figsize=(10, 6))
    plt.bar(hour_usage["hour"], hour_usage[usage_col], color="#1f77b4")
    plt.title("Busiest Hours for Charging Stations", fontsize=14)
    plt.xlabel("Hour of Day (0-23)", fontsize=12)
    plt.ylabel("Total Occupied Connectors / Sessions", fontsize=12)
    plt.xticks(range(24))
    plt.tight_layout()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    chart_filename = f"busiest_hours_{timestamp}.png"
    chart_path = os.path.join(output_dir, chart_filename)
    plt.savefig(chart_path)
    plt.close()
    logging.info(f"Busiest hours chart saved to {chart_path}")
    return chart_path



# Main Function

def main():
    parser = argparse.ArgumentParser(
        description="Generate EV Charging Stations Visualizations: Map, Connector Distribution Pie Chart, and Busiest Hours Chart"
    )
    parser.add_argument("--data-dir", type=str, default=DEFAULT_DATA_DIR,
                        help="Directory containing CSV data")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR,
                        help="Directory to save visualizations")
    parser.add_argument("--log-level", type=str, default="INFO",
                        help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s [%(levelname)s] %(message)s")

    # Load data
    stations_df = load_stations_data(args.data_dir)
    hourly_df = load_hourly_data(args.data_dir)

    if stations_df is None:
        logging.error("No stations data loaded. Exiting.")
        return 1

    # Create visualizations
    saved_files = []

    map_path = create_map_visualization(stations_df, args.output_dir)
    if map_path:
        saved_files.append(map_path)

    pie_path = create_connector_type_distribution(stations_df, args.output_dir)
    if pie_path:
        saved_files.append(pie_path)

    busiest_path = create_busiest_hours_chart(hourly_df, args.output_dir)
    if busiest_path:
        saved_files.append(busiest_path)

    logging.info("Visualizations generated:")
    for path in saved_files:
        logging.info(f"- {path}")

    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
