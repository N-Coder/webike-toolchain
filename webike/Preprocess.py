import logging
from datetime import timedelta

from webike.data import SoC
from webike.data import Trips
from webike.data import WeatherGC
from webike.data import WeatherWU
from webike.data.ChargeCycle import preprocess_cycles
from webike.util import DB
from webike.util.Utils import smooth, smooth_reset_stale, differentiate

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")


def smooth_func(samples, charge_attr):
    return smooth(samples, charge_attr, is_valid=smooth_reset_stale(timedelta(minutes=5))), charge_attr


def preprocess_soc_func(samples, charge_attr):
    attr_diff = charge_attr + '_diff'
    samples = differentiate(samples, charge_attr, label_diff=attr_diff, delta_time=timedelta(hours=1))
    attr_smooth = attr_diff + '_smooth'
    samples = smooth(samples, attr_diff, label_smooth=attr_smooth, is_valid=smooth_reset_stale(timedelta(minutes=5)))
    return samples, attr_smooth


def main():
    with DB.connect() as connection:
        SoC.preprocess_estimates(connection)
        connection.commit()

        preprocess_cycles(connection, charge_attr='ChargingCurr',
                          charge_thresh_start=(lambda x: x > 50), charge_thresh_end=(lambda x: x < 50))
        preprocess_cycles(connection, charge_attr='DischargeCurr', preprocess_func=smooth_func,
                          charge_thresh_start=(lambda x: x < 490), charge_thresh_end=(lambda x: x > 490))
        preprocess_cycles(connection, charge_attr='soc_smooth', preprocess_func=preprocess_soc_func,
                          charge_thresh_start=(lambda x: x < 8), charge_thresh_end=(lambda x: x > 2))
        connection.commit()
        # TODO merge detected cycles or only use one method

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
