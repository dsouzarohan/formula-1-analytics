import yaml


def get_db_config():
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    print(config)

    return config['DATABASE']
