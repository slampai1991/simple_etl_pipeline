import logging
from main import load_config

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration
config = load_config()

def load():
    """Load the transformed data into destinations."""
    for destination in config['data_destinations']:
        try:
            logging.info(f"Loading data into destination: {destination['name']}")
            if destination['type'] == 'postgres':
                # Add logic to load data into PostgreSQL here
                pass
            elif destination['type'] == 'file':
                # Add logic to save data to a file here
                pass
            else:
                logging.warning(f"Unknown destination type: {destination['type']}")
        except Exception as e:
            logging.error(f"Error loading data into {destination['name']}: {e}")

if __name__ == "__main__":
    load()