import os
import yaml

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Points to root directory
SQL_DIR = os.path.join(ROOT_DIR, 'pgsql')  # Targets /pgsql directory, SQL directory
BASE_DIR = os.path.join(ROOT_DIR, 'src')  # Targets /src directory, code directory
DATA_PATH = os.path.join(ROOT_DIR, 'data', 'external')  # Targets /data/external, CSVs are here
LOGS_PATH = os.path.join(ROOT_DIR, 'logs')  # Targets /logs for logs
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
