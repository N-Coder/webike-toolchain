import copy
import logging
from datetime import timedelta

import matplotlib.pyplot as plt

from webike.util.Constants import IMEIS, STUDY_START
from webike.util.DB import DictCursor, StreamingDictCursor, QualifiedDictCursor
from webike.util.Logging import BraceMessage as __
from webike.util.Plot import to_hour_bin, hist_day_hours, hist_year_months, hist_week_days
from webike.util.Utils import zip_prev, progress

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")

HIST_DATA = {'start_times': [], 'end_times': [], 'durations': [], 'initial_soc': [], 'final_soc': [],
             'start_weekday': [], 'start_month': []}


def extract_cycles_curr(charge_samples, charge_attr, charge_thresh_start, charge_thresh_end,
                        min_charge_samples, max_sample_delay, min_charge_time):
    """Detect charging cycles based on the ChargingCurr."""
    cycles = []
    discarded_cycles = []
    charge_start = charge_end = None
    charge_sample_count = 0
    charge_avg = 0

    charge_samples = progress(charge_samples, logger=logger,
                              msg="Processed {countf} samples after {timef}s ({ratef} samples per second)")
    for last_sample, sample in zip_prev(charge_samples):
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
    return cycles, discarded_cycles


def preprocess_cycles(connection, charge_attr, charge_thresh_start, charge_thresh_end, smooth_func=None, type=None,
                      min_charge_samples=100, max_sample_delay=timedelta(minutes=10),
                      min_charge_time=timedelta(minutes=10)):
    if not type:
        type = charge_attr[0]
    with connection.cursor(DictCursor) as cursor:
        for nr, imei in enumerate(IMEIS):
            logger.info(__("Preprocessing charging cycles for {}", imei))

            # reprocess the last detected cycle, as it could have been cut of by data that wasn't uploaded yet
            start_time = STUDY_START
            last_cycle = None
            with connection.cursor(StreamingDictCursor) as scursor:
                scursor.execute(
                    "SELECT start_time, end_time "
                    "FROM webike_sfink.charge_cycles "
                    "WHERE imei='{imei}' AND type='{type}' "
                    "ORDER BY start_time DESC;"
                        .format(imei=imei, type=type))
                for cycle in scursor.fetchall_unbuffered():
                    # if the latest 2 cycles are close together, go back further just to be sure
                    if not last_cycle or cycle['end_time'] > start_time:
                        start_time = cycle['start_time'] - timedelta(hours=1)
                        last_cycle = cycle
                    else:
                        break

            # use another streaming cursor as the first one wasn't completely consumed
            with connection.cursor(StreamingDictCursor) as scursor:
                # fetch the charging sensor data and prepare the raw values
                scursor.execute(
                    """SELECT Stamp, ChargingCurr, DischargeCurr, BatteryVoltage, soc_smooth FROM imei{imei}
                    JOIN webike_sfink.soc ON Stamp = time AND imei = '{imei}'
                    WHERE {attr} IS NOT NULL AND {attr} != 0 AND Stamp >= '{start_time}'
                    ORDER BY Stamp ASC"""
                        .format(imei=imei, attr=charge_attr, start_time=start_time))
                charge = scursor.fetchall_unbuffered()
                if callable(smooth_func):
                    charge = smooth_func(charge, charge_attr)

                logger.info(__("Detecting charging cycles after {} based on {}", start_time, charge_attr))
                cycles_curr, cycles_curr_disc = \
                    extract_cycles_curr(charge, charge_attr, charge_thresh_start, charge_thresh_end,
                                        min_charge_samples, max_sample_delay, min_charge_time)

            # delete outdated cycles and write newly detected ones
            logger.info(__("Writing {} detected cycles to DB with label '{}', discarded {} cycles",
                           len(cycles_curr), type, len(cycles_curr_disc)))
            cursor.execute(
                "DELETE FROM webike_sfink.charge_cycles "
                "WHERE imei='{imei}' AND start_time >= '{start_time}' AND type='{type}';"
                    .format(imei=imei, start_time=start_time, type=type))
            cursor.executemany(
                """INSERT INTO webike_sfink.charge_cycles
                (imei, start_time, end_time, sample_count, avg_thresh_val, type)
                VALUES (%s, %s, %s, %s, %s, %s);""",
                [[imei, cycle[0]['Stamp'], cycle[1]['Stamp'], cycle[2], cycle[3], type]
                 for cycle in cycles_curr]
            )


def extract_hist(connection):
    logger.info("Generating charge cycle histogram data")

    with connection.cursor(QualifiedDictCursor) as qcursor:
        hist_data = copy.deepcopy(HIST_DATA)

        for imei in IMEIS:
            logger.info(__("Processing IMEI {}", imei))

            qcursor.execute(
                "SELECT * "
                "FROM webike_sfink.charge_cycles cc "
                "  LEFT OUTER JOIN imei{imei} first_sample ON first_sample.Stamp = cc.start_time "
                "  LEFT OUTER JOIN imei{imei} last_sample ON last_sample.Stamp = cc.end_time "
                "  LEFT OUTER JOIN webike_sfink.soc first_soc ON first_soc.time = cc.start_time"
                "                                            AND first_soc.imei = '{imei}' "
                "  LEFT OUTER JOIN webike_sfink.soc last_soc ON last_soc.time = cc.end_time"
                "                                           AND last_soc.imei = '{imei}' "
                "WHERE cc.imei = '{imei}'".format(imei=imei))
            trips = qcursor.fetchall()
            for trip in progress(trips):
                hist_data['durations'].append(trip['last_sample.Stamp'] - trip['first_sample.Stamp'])
                hist_data['start_times'].append(to_hour_bin(trip['first_sample.Stamp']))
                hist_data['end_times'].append(to_hour_bin(trip['last_sample.Stamp']))
                hist_data['start_weekday'].append(trip['first_sample.Stamp'].weekday())
                hist_data['start_month'].append(trip['first_sample.Stamp'].month)

                hist_data['initial_soc'].append(float(trip['first_soc.soc_smooth']))
                hist_data['final_soc'].append(float(trip['last_soc.soc_smooth']))

        return hist_data


def plot_charge_cycles(hist_data):
    logger.info("Plotting charge cycle graphs")
    plt.clf()
    hist_day_hours(plt.gca(), hist_data['start_times'])
    plt.xlabel("Time of Day")
    plt.ylabel("Number of started Charge Cycles")
    plt.title("Number of started Charge Cycles per Hour of Day")
    plt.savefig("out/charge_start_per_hour.png")

    plt.clf()
    hist_day_hours(plt.gca(), hist_data['end_times'])
    plt.xlabel("Time of Day")
    plt.ylabel("Number of ended Charge Cycles")
    plt.title("Number of ended Charge Cycles per Hour of Day")
    plt.savefig("out/charge_end_per_hour.png")

    plt.clf()
    hist_week_days(plt.gca(), hist_data['start_weekday'])
    plt.xlabel("Weekday")
    plt.ylabel("Number of Charge Cycles")
    plt.title("Number of Charge Cycles per Weekday")
    plt.savefig("out/charge_per_weekday.png")

    plt.clf()
    hist_year_months(plt.gca(), hist_data['start_month'])
    plt.xlabel("Month")
    plt.ylabel("Number of Charge Cycles")
    plt.title("Number of Charge Cycles per Month")
    plt.savefig("out/charge_per_month.png")

    plt.clf()
    plt.hist([x / timedelta(minutes=1) for x in hist_data['durations']], range=(0, 1800), bins=18)
    plt.xlabel("Duration in Minutes")
    plt.ylabel("Number of Charge Cycles")
    plt.title("Number of Charge Cycles per Duration")
    plt.savefig("out/charge_per_duration.png")
