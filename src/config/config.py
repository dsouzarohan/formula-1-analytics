import os
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Targets /src directory
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.yaml')  # Targets config.yaml


def load_config() -> tuple[dict, None] | tuple[None, str]:
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        return config, None
    except FileNotFoundError:
        error = f"Config file not found in {CONFIG_PATH}"
        return None, error
    except yaml.YAMLError as yaml_error:
        error = f"Error in configuration file: {yaml_error}"
        return None, error


db_config, error = load_config()
