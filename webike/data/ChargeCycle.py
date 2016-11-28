import logging
from datetime import timedelta

from iss4e.db.mysql import DictCursor, StreamingDictCursor
from iss4e.util import BraceMessage as __
from tabulate import tabulate
from webike.util.activity import ActivityDetection, Cycle
from webike.util.constants import IMEIS, STUDY_START, TD0

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


class ChargeCycleDetection(ActivityDetection):
    def __init__(self, attr, sql_attr=None, min_sample_count=100, min_cycle_duration=timedelta(minutes=10)):
        self.attr = attr
        if not sql_attr:
            sql_attr = attr
        self.sql_attr = sql_attr
        self.min_sample_count = min_sample_count
        self.min_cycle_duration = min_cycle_duration
        super().__init__()

    def accumulate_samples(self, new_sample, accumulator):
        if 'avg' in accumulator:
            accumulator['avg'] = (accumulator['avg'] + new_sample[self.attr]) / 2
        else:
            accumulator['avg'] = new_sample[self.attr]

        if 'cnt' not in accumulator:
            accumulator['cnt'] = 0
        accumulator['cnt'] += 1
        return accumulator

    def check_reject_reason(self, cycle: Cycle):
        if cycle.stats['cnt'] < self.min_sample_count:
            return "acc_cnt<{}".format(self.min_sample_count)
        elif self.get_duration(cycle.start, cycle.end) < self.min_cycle_duration:
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
                        .format(imei=imei, attr=detector.sql_attr, start_time=start_time))
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
                [[imei, cycle.start['Stamp'], cycle.end['Stamp'], cycle.stats['cnt'], cycle.stats['avg'], type]
                 for cycle in cycles_curr]
            )

    logger.debug(__("Results of preprocessing charging cycles using {}:\n{}", detector,
                    tabulate([(imei, len(cycles[imei][0]), len(cycles[imei][1])) for imei in cycles],
                             headers=("imei", "accepted", "discarded"))))

    return cycles
