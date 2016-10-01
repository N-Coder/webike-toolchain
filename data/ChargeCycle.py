import logging
from datetime import timedelta

from util import DB
from util.Constants import IMEIS
from util.DB import DictCursor
from util.Logging import BraceMessage as __
from util.Utils import smooth

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")


def extract_cycles_curr(charge_samples, charge_attr, charge_thresh_start, charge_thresh_end,
                        min_charge_samples=100, max_sample_delay=timedelta(minutes=10),
                        min_charge_time=timedelta(minutes=10)):
    """Detect charging cycles based on the ChargingCurr."""
    cycles = []
    discarded_cycles = []
    charge_start = charge_end = None
    charge_sample_count = 0
    charge_avg = 0

    last_sample = None
    for sample in charge_samples:
        # did charging start?
        if not charge_start:
            if charge_thresh_start(sample[charge_attr]):
                # yes, because ChargingCurr is high
                charge_start = sample
                charge_sample_count = 1
                charge_avg = sample[charge_attr]

        # did charging stop?
        else:
            if charge_thresh_end(sample[charge_attr]):
                # yes, because ChargingCurr is back to normal
                charge_end = last_sample
            elif sample['Stamp'] - last_sample['Stamp'] > max_sample_delay:
                # yes, because we didn't get a sample for the last few mins
                charge_end = last_sample
            else:
                # nope, continue counting
                charge_sample_count += 1
                charge_avg = (charge_avg + sample[charge_attr]) / 2

            if charge_end:
                cycle = (charge_start, charge_end, charge_sample_count, charge_avg)
                # only count as charging cycle if it lasts for more than a few mins, we got enough samples
                # and we actually increased the SoC
                if charge_end['Stamp'] - charge_start['Stamp'] > min_charge_time \
                        and charge_sample_count > min_charge_samples:
                    cycles.append(cycle)
                else:
                    discarded_cycles.append(cycle)
                charge_start = None
                charge_end = None
                charge_sample_count = 0
                charge_avg = 0
        last_sample = sample
    return cycles, discarded_cycles


def preprocess_cycles(connection, charge_attr, charge_thresh_start, charge_thresh_end, smooth_func=None):
    with connection.cursor(DictCursor) as cursor:
        for nr, imei in enumerate(IMEIS):
            logger.info(__("Preprocessing charging cycles for {}", imei))
            cursor.execute(
                """SELECT Stamp, ChargingCurr, DischargeCurr, BatteryVoltage, soc_smooth FROM imei{imei}
                JOIN webike_sfink.soc ON Stamp = time AND imei = '{imei}'
                WHERE {attr} IS NOT NULL AND {attr} != 0
                ORDER BY Stamp ASC"""
                    .format(imei=imei, attr=charge_attr))
            charge = cursor.fetchall()

            if callable(smooth_func):
                smooth_func(charge, charge_attr)

            logger.info(__("Detecting charging cycles based on {}", charge_attr))
            cycles_curr, cycles_curr_disc = \
                extract_cycles_curr(charge, charge_attr, charge_thresh_start, charge_thresh_end)

            logger.info(__("Writing {} detected cycles to DB with label '{}', discarded {} cycles",
                           len(cycles_curr), charge_attr[0], len(cycles_curr_disc)))
            for cycle in cycles_curr:
                cursor.execute(
                    """INSERT INTO webike_sfink.charge_cycles
                    (imei, start_time, end_time, sample_count, avg_thresh_val, type)
                    VALUES (%s, %s, %s, %s, %s, %s);""",
                    [imei, cycle[0]['Stamp'], cycle[1]['Stamp'], cycle[2], cycle[3], charge_attr[0]])


# TODO start/end time, duration, Initial/Final State of Charge
# TODO preprocess incremental changes and move to Preprocess.py

def smooth_func(samples, charge_attr):
    smooth(samples, charge_attr, is_valid=lambda sample, last_sample, label: \
        last_sample and last_sample['Stamp'] - sample['Stamp'] < timedelta(minutes=5))


with DB.connect() as mconnection:
    with mconnection.cursor(DictCursor) as mcursor:
        mcursor.execute("DELETE FROM webike_sfink.charge_cycles WHERE type='D';")
    # preprocess_cycles(mconnection, charge_attr='ChargingCurr',
    #                  charge_thresh_start=(lambda x: x > 50), charge_thresh_end=(lambda x: x < 50))
    preprocess_cycles(mconnection, charge_attr='DischargeCurr', smooth_func=smooth_func,
                      charge_thresh_start=(lambda x: x < 490), charge_thresh_end=(lambda x: x > 490))
    mconnection.commit()
