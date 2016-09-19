import logging

import DB
import SoC
import Trips
import WeatherGC
import WeatherWU

__author__ = "Niko Fink"
assert __name__ == "__main__"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")

with DB.connect() as connection:
    SoC.preprocess_estimates(connection)
    connection.commit()

    Trips.preprocess_trips(connection)
    connection.commit()

    gc_files = WeatherGC.download_data()
    gc_csv_data = WeatherGC.parse_data(gc_files)
    WeatherGC.write_data_csv(gc_csv_data)
    WeatherGC.write_data_db(connection, gc_csv_data)
    connection.commit()

    wu_missing_data = WeatherWU.select_missing_dates(connection)
    WeatherWU.download_wunderg(connection, wu_missing_data)
    connection.commit()
