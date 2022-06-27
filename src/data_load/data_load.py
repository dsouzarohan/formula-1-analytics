from src.data_load import circuits_load as circuits, drivers_load as drivers


def load_data():
    circuits.load()
    drivers.load()
