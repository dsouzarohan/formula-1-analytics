import yaml


def get_db_config():
    # TODO: Need to reference this file without using the absolute system path
    with open('D:\\Documents\\Python Projects\\formula-1-analytics\\src\\config\\config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    return config['DATABASE']
