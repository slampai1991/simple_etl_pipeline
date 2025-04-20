import yaml
import os


def load_config(config_path="config.yaml"):
    """Load configuration from YAML file

    Args:
        config_path (str, optional): Path to the configuration file. Defaults to "config.yaml".

    Raises:
        FileNotFoundError: If the configuration file is not found.

    Returns:
        _type_: Configuration dictionary.
    """
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
