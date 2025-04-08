import requests
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_charging_stations(url="https://charging.eviny.no/api/map/chargingStations",
                              max_retries=3, retry_delay=5):

    logger.info(f"Extracting charging stations data from {url}")

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Parse the JSON response
            data = response.json()

            logger.info(f"Response data type: {type(data)}")

            if isinstance(data, dict) and "chargingStations" in data:
                # The API returns a dictionary with a "chargingStations" key containing the list
                stations_list = data["chargingStations"]
                logger.info(f"Extracted {len(stations_list)} stations from 'chargingStations' key")
            elif isinstance(data, list):
                # The API returns a list directly
                stations_list = data
                logger.info(f"Extracted {len(stations_list)} stations from list response")
            else:
                logger.error(f"Unexpected data format: {type(data)}")

                # save the response for debugging
                try:
                    import json
                    with open('api_response_debug.json', 'w') as f:
                        json.dump(data, f, indent=2)
                    logger.info("Saved API response to 'api_response_debug.json' for debugging")
                except Exception as e:
                    logger.error(f"Could not save debug file: {e}")

                return None

            # Process stations data to handle the "connectionsTypes" field
            processed_stations = []
            for station in stations_list:
                # Standardize field names
                if "connectionsTypes" in station and "connectionTypes" not in station:
                    station["connectionTypes"] = station.pop("connectionsTypes")

                # Process connector information
                if "connectionTypes" in station:
                    connectors = []
                    for conn_type, conn_list in station["connectionTypes"].items():
                        for connector in conn_list:
                            # Add the connector type to each connector
                            connector["type"] = conn_type
                            connectors.append(connector)

                    # Add the processed connectors list
                    station["connectors"] = connectors

                processed_stations.append(station)

            logger.info(f"Successfully processed {len(processed_stations)} stations")
            return processed_stations

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error on attempt {attempt + 1}/{max_retries + 1}: {e}")

            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries exceeded. Extraction failed.")
                return None

        except ValueError as e:
            logger.error(f"Error parsing JSON response: {e}")
            return None

        except Exception as e:
            logger.error(f"Unexpected error during extraction: {e}")
            return None


if __name__ == "__main__":
    # Test the extraction function directly
    stations = extract_charging_stations()

    if stations:
        print(f"Successfully extracted {len(stations)} stations")
        print(f"First station: {stations[0]['name']}")

        # Check connectors
        if "connectors" in stations[0]:
            connectors = stations[0]["connectors"]
            print(f"First station has {len(connectors)} connectors")
            if connectors:
                print(f"First connector: {connectors[0]}")
    else:
        print("Extraction failed")