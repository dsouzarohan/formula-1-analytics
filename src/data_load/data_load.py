from src.data_load import circuits_load as circuits, drivers_load as drivers, constructors_load as constructors, \
    status_load as status, seasons_load as seasons, races_load as race, qualifying_load as qualifying, \
    sprint_results_load as sprint_results, lap_times_load as lap_times, pit_stops_load as pit_stops, \
    results_load as results


def load_data():
    # First hierarchy of data

    # circuits.load()
    # drivers.load()
    # constructors.load()
    # status.load()
    # seasons.load()

    # Second hierarchy of data

    # race.load()
    # qualifying.load()
    # sprint_results.load()
    # lap_times.load()
    # pit_stops.load()

    # Third hierarchy of data
    results.load()


