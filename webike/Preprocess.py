import logging
from datetime import timedelta

from webike.data import SoC
from webike.data import Trips
from webike.data import WeatherGC
from webike.data import WeatherWU
from webike.data.ChargeCycle import preprocess_cycles, ChargeCycleDetection
from webike.util import DB
from webike.util.Utils import smooth, smooth_reset_stale, differentiate

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")


class ChargingCurrCCDetection(ChargeCycleDetection):
    def __init__(self, *args, **kwargs):
        super().__init__('ChargingCurr', *args, **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] < 50

    def is_end(self, sample, previous):
        return sample[self.attr] > 50 or self.get_duration(previous, sample) > timedelta(minutes=10)


class DischargeCurrCCDetection(ChargeCycleDetection):
    def __init__(self, *args, **kwargs):
        super().__init__('DischargeCurr_smooth', *args, **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] < 490

    def is_end(self, sample, previous):
        return sample[self.attr] > 490 or self.get_duration(previous, sample) > timedelta(minutes=10)

    def __call__(self, cycle_samples):
        cycle_samples = smooth(cycle_samples, 'DischargeCurr', is_valid=smooth_reset_stale(timedelta(minutes=5)))
        return super(self)(cycle_samples)


class SoCDerivCCDetection(ChargeCycleDetection):
    def __init__(self, *args, **kwargs):
        super().__init__('soc_smooth_diff', *args, **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] < 8

    def is_end(self, sample, previous):
        return sample[self.attr] > 2 or self.get_duration(previous, sample) > timedelta(minutes=10)

    def __call__(self, cycle_samples):
        cycle_samples = differentiate(cycle_samples, 'soc_smooth', delta_time=timedelta(hours=1))
        return super(self)(cycle_samples)


def main():
    with DB.connect() as connection:
        SoC.preprocess_estimates(connection)
        connection.commit()

        preprocess_cycles(connection, ChargingCurrCCDetection())
        preprocess_cycles(connection, DischargeCurrCCDetection())
        preprocess_cycles(connection, SoCDerivCCDetection())
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
