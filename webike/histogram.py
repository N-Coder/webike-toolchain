from iss4e.db import mysql
from iss4e.util.config import load_config

from webike.data import ChargeCycle
from webike.data import Trips
from webike.data import WeatherGC
from webike.data import WeatherWU
from webike.util import plot

__author__ = "Niko Fink"


def main():
    config = load_config()
    with mysql.connect(**config['webike.mysql']) as connection:
        trip_hist_data = Trips.extract_hist(connection)
        Trips.plot_trips(trip_hist_data)

        charge_hist_data = ChargeCycle.extract_hist(connection)
        ChargeCycle.plot_charge_cycles(charge_hist_data)

        gc_db_data = WeatherGC.read_data_db(connection)
        gc_hist_data = WeatherGC.extract_hist(gc_db_data)
        plot.plot_weather(
            {
                'weather': gc_hist_data,
                'trip': trip_hist_data['trip_weather']
            },
            out_file="out/trips_per_weathergc_{}.png"
        )

        wu_db_data = WeatherWU.read_data_db(connection)
        wu_hist_data = WeatherWU.extract_hist(wu_db_data)
        plot.plot_weather(
            {
                'weather': wu_hist_data,
                'trip': trip_hist_data['trip_metar']
            },
            out_file="out/trips_per_wunderg_{}.png"
        )


if __name__ == "__main__":
    main()
