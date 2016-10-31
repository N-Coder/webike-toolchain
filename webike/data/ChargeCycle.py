import copy
import logging
from datetime import timedelta

import matplotlib.pyplot as plt
from tabulate import tabulate
from webike.util import ActivityDetection
from webike.util.Constants import IMEIS, STUDY_START, TD0
from webike.util.DB import DictCursor, StreamingDictCursor, QualifiedDictCursor
from webike.util.Logging import BraceMessage as __
from webike.util.Plot import to_hour_bin, hist_day_hours, hist_year_months, hist_week_days
from webike.util.Utils import progress

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")

HIST_DATA = {'start_times': [], 'end_times': [], 'durations': [], 'initial_soc': [], 'final_soc': [],
             'start_weekday': [], 'start_month': []}


class ChargeCycleDetection(ActivityDetection):
    def __init__(self, attr, min_sample_count=100, min_cycle_duration=timedelta(minutes=10)):
        self.attr = attr
        self.min_sample_count = min_sample_count
        self.min_cycle_duration = min_cycle_duration
        super().__init__()

    def accumulate_samples(self, new_sample, accumulator):
        if accumulator is not None:
            avg, cnt = accumulator
            return (avg + new_sample[self.attr]) / 2, cnt + 1
        else:
            return new_sample[self.attr], 1

    def check_reject_reason(self, cycle):
        cycle_start, cycle_end, cycle_acc = cycle
        acc_avg, acc_cnt = cycle_acc
        if acc_cnt < self.min_sample_count:
            return "acc_cnt<{}".format(self.min_sample_count)
        elif self.get_duration(cycle_start, cycle_end) < self.min_cycle_duration:
            return "duration<{}".format(self.min_cycle_duration)
        else:
            return None

    @staticmethod
    def get_duration(first, second):
        dur = second['Stamp'] - first['Stamp']
        assert dur >= TD0, "second sample {} happened before first {}".format(second, first)
        return dur


def preprocess_cycles(connection, detector: ChargeCycleDetection, type=None):
    if not type:
        type = detector.attr[0]
    logger.debug(__("Preprocessing charging cycles using {}", detector))

    cycles = {}
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
                        .format(imei=imei, attr=detector.attr, start_time=start_time))
                charge = scursor.fetchall_unbuffered()

                logger.info(__("Detecting charging cycles after {} using {}", start_time, detector))
                cycles_curr, cycles_curr_disc = detector(charge)
                cycles[imei] = (cycles_curr, cycles_curr_disc)

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

    logger.debug(__("Results of preprocessing charging cycles using {}:\n{}", detector,
                    tabulate([(imei, len(cycles[imei][0]), len(cycles[imei][1])) for imei in cycles],
                             headers=("imei", "accepted", "discarded"))))

    return cycles


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
