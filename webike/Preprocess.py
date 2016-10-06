import logging
from datetime import timedelta

from webike.data import SoC
from webike.data import Trips
from webike.data import WeatherGC
from webike.data import WeatherWU
from webike.data.ChargeCycle import preprocess_cycles
from webike.util import DB
from webike.util.Utils import smooth

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")


def smooth_func(samples, charge_attr):
    return smooth(samples, charge_attr, is_valid=lambda sample, last_sample, label: \
        last_sample and last_sample['Stamp'] - sample['Stamp'] < timedelta(minutes=5))


def main():
    with DB.connect() as connection:
        SoC.preprocess_estimates(connection)
        connection.commit()

        preprocess_cycles(connection, charge_attr='ChargingCurr',
                          charge_thresh_start=(lambda x: x > 50), charge_thresh_end=(lambda x: x < 50))
        preprocess_cycles(connection, charge_attr='DischargeCurr', smooth_func=smooth_func, min_charge_samples=1,
                          charge_thresh_start=(lambda x: x < 490), charge_thresh_end=(lambda x: x > 490))
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


if __name__ == "__main__":
    main()
