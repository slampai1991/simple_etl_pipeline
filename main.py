import yaml
import os

def load_config(config_path="config.yaml"):
    """Load configuration from a YAML file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

# Example usage
if __name__ == "__main__":
    config = load_config()
    print("Loaded configuration:", config)

# Main script to run the ETL process