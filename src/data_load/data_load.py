from src.data_load import circuits_load as circuits, drivers_load as drivers, constructors_load as constructors, \
    status_load as status, seasons_load as seasons, races_load as race, qualifying_load as qualifying, \
    sprint_results_load as sprint_results, lap_times_load as lap_times, pit_stops_load as pit_stops, \
    results_load as results, constructors_results_load as constructors_results, \
    constructor_standings_load as constructor_standings, driver_standings_load as driver_standings
from src.utilities.logger import create_log, log_data_load


def load_data():
    start = create_log()
    # First hierarchy of data

    circuits.load()
    drivers.load()
    constructors.load()
    status.load()
    seasons.load()

    # Second hierarchy of data

    race.load()
    qualifying.load()
    sprint_results.load()
    lap_times.load()
    pit_stops.load()

    # Third hierarchy of data

    results.load()
    constructors_results.load()
    constructor_standings.load()
    driver_standings.load()

    log_data_load(None, 'FINAL', start, None)

