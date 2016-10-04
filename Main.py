import logging

from data import ChargeCycle
from data import Trips
from data import WeatherGC
from data import WeatherWU
from util import DB
from util import Plot

__author__ = "Niko Fink"
assert __name__ == "__main__"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")

with DB.connect() as connection:
    trip_hist_data = Trips.extract_hist(connection)
    Trips.plot_trips(trip_hist_data)

    charge_hist_data = ChargeCycle.extract_hist(connection)
    ChargeCycle.plot_charge_cycles(charge_hist_data)

    gc_db_data = WeatherGC.read_data_db(connection)
    gc_hist_data = WeatherGC.extract_hist(gc_db_data)
    Plot.plot_weather(
        {
            'weather': gc_hist_data,
            'trip': trip_hist_data['trip_weather']
        },
        out_file="out/trips_per_weathergc_{}.png"
    )

    wu_db_data = WeatherWU.read_data_db(connection)
    wu_hist_data = WeatherWU.extract_hist(wu_db_data)
    Plot.plot_weather(
        {
            'weather': wu_hist_data,
            'trip': trip_hist_data['trip_metar']
        },
        out_file="out/trips_per_wunderg_{}.png"
    )
