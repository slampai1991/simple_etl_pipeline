import logging
from main import load_config

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration
config = load_config()

def transform():
    """Transform the extracted data."""
    for transformation in config['transformations']:
        try:
            logging.info(f"Applying transformation: {transformation['name']}")
            if transformation['type'] == 'drop_nulls':
                # Add logic to drop nulls here
                pass
            elif transformation['type'] == 'calculate':
                # Add logic to calculate new columns here
                pass
            else:
                logging.warning(f"Unknown transformation type: {transformation['type']}")
        except Exception as e:
            logging.error(f"Error applying transformation {transformation['name']}: {e}")

if __name__ == "__main__":
    transform()