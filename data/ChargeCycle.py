import collections
import logging
from datetime import timedelta, datetime, date

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta

from util import DB
from util.Constants import IMEIS
from util.DB import DictCursor
from util.Logging import BraceMessage as __

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")


# TODO start/end time, duration, Initial/Final State of Charge

def daterange(start, stop=datetime.now(), step=timedelta(days=1)):
    """Similar to :py:func:`builtins.range`, but for dates"""
    if start < stop:
        cmp = lambda a, b: a < b
        inc = lambda a: a + step
    else:
        cmp = lambda a, b: a > b
        inc = lambda a: a - step
    yield start
    while cmp(start, stop):
        start = inc(start)
        yield start


def smooth(samples, label, label_smooth=None, alpha=.95, default_value=None):
    """Smooth values using the formula
    `samples[n][label_smooth] = alpha * samples[n-1][label_smooth] + (1 - alpha) * samples[n][label]`
    If a value isn't available, the previous smoothed value is used.
    If none of these exist, default_value is used
    :param samples: a list of dicts
    :param label:
    :param label_smooth:
    :param alpha:
    :param default_value:
    :return:
    """
    if not label_smooth:
        label_smooth = label + '_smooth'

    last_sample = None
    for sample in samples:
        if not (sample and label in sample and sample[label]):
            if not (last_sample and label_smooth in last_sample and
                        last_sample[label_smooth]):
                # don't have any values yet, use default
                sample[label_smooth] = default_value
            else:
                # don't have a value for this sample, keep previous smoothed values
                sample[label_smooth] = last_sample[label_smooth]
        else:
            if not (last_sample and label_smooth in last_sample and
                        last_sample[label_smooth]):
                # 1nd sensible value in the list, use it as starting point for the smoothing
                sample[label_smooth] = sample[label]
            else:
                # current and previous value available, apply the smoothing function
                sample[label_smooth] = alpha * last_sample[label_smooth] \
                                       + (1 - alpha) * sample[label]
        last_sample = sample


def discharge_curr_to_ampere(val):
    """Convert DischargeCurr from the DB from the raw sensor value to amperes"""
    return (val - 504) * 0.033 if val else 0


def extract_cycles_curr(charge_samples,
                        charge_thresh_start=50, charge_thresh_end=50,
                        max_sample_delay=timedelta(minutes=10), min_charge_time=timedelta(minutes=10)):
    """Detect charging cycles based on the smoothed ChargingCurr."""
    cycles = []
    discarded_cycles = []
    charge_start = charge_end = None

    last_sample = None
    for sample in charge_samples:
        # did charging start?
        if not charge_start:
            if sample['ChargingCurr_smooth'] > charge_thresh_start:
                # yes, because ChargingCurr is high
                charge_start = sample

        # did charging stop?
        else:
            if sample['ChargingCurr_smooth'] < charge_thresh_end:
                # yes, because ChargingCurr is back to normal
                charge_end = sample
            elif sample['Stamp'] - last_sample['Stamp'] > max_sample_delay:
                # yes, because we didn't get a sample for the last few mins
                charge_end = last_sample

            if charge_end:
                # only count as charging cycle if it lasts for more than a few mins
                if charge_end['Stamp'] - charge_start['Stamp'] > min_charge_time:
                    cycles.append((charge_start, charge_end))
                else:
                    discarded_cycles.append((charge_start, charge_end))
                charge_start = None
                charge_end = None
        last_sample = sample
    return cycles, discarded_cycles


def can_merge(cycles, new_start, new_end, merge_within):
    if len(cycles) < 1: return False
    last_start, last_end = cycles[-1]
    gap = new_start['Stamp'] - last_end['Stamp']
    # only merge if the time gap between the two cycles is less than merge_within
    if gap > merge_within: return False
    # don't merge small samples with a big gap between them
    if new_end['Stamp'] - new_start['Stamp'] < gap: return False
    if last_end['Stamp'] - last_start['Stamp'] < gap: return False
    return True


def extract_cycles_soc(charge_samples,
                       derivate_span=10, charge_thresh_start=0.001, charge_thresh_end=0.001,
                       max_sample_delay=timedelta(minutes=10), min_charge_time=timedelta(minutes=30),
                       min_charge_amount=0.05, merge_within=timedelta(minutes=30)):
    """Detect charging cycles based on an increasing state of charge."""
    cycles = []
    discarded_cycles = []
    charge_start = charge_end = None

    soc_history = collections.deque(maxlen=derivate_span)
    last_sample = None
    for sample in charge_samples:
        # estimate the derivation of SoC by comparing
        # the average of the first half of the last `derivate_span` samples with
        # the average of the second half
        soc_history.append(sample['soc_smooth'])
        l = len(soc_history)
        if l >= soc_history.maxlen:
            h = list(soc_history)
            old_avg = sum(h[0:l // 2]) / len(h[0:l // 2])
            new_avg = sum(h[l // 2:l]) / len(h[l // 2:l])
            sample['soc_diff'] = new_avg - old_avg
        else:
            sample['soc_diff'] = 0

        # did charging start?
        if not charge_start:
            if sample['soc_diff'] > charge_thresh_start:
                # yes, because SoC is increasing
                charge_start = sample

        # did charging stop?
        else:
            if sample['soc_diff'] < charge_thresh_end:
                # yes, because SoC isn't increasing anymore
                charge_end = sample
            elif sample['Stamp'] - last_sample['Stamp'] > max_sample_delay:
                # yes, because we didn't get a sample for the last few mins
                charge_end = last_sample

            if charge_end:
                if can_merge(cycles, charge_start, charge_end, merge_within):
                    # merge with previous cycle if they are close together
                    cycles[-1] = (cycles[-1][0], charge_end)
                else:
                    if can_merge(discarded_cycles, charge_start, charge_end, merge_within):
                        # merge with previous discarded cycle if they are close together
                        # and check again whether they should be added altogether
                        charge_start = discarded_cycles[-1][0]
                        del discarded_cycles[-1]

                    if charge_end['Stamp'] - charge_start['Stamp'] > min_charge_time \
                            and charge_end['soc_smooth'] - charge_start['soc_smooth'] > min_charge_amount:
                        # only count as charging cycle if it lasts for more than a few mins
                        # and actually increased the SoC
                        cycles.append((charge_start, charge_end))
                    else:
                        discarded_cycles.append((charge_start, charge_end))

                charge_start = None
                charge_end = None

        last_sample = sample
    return cycles, discarded_cycles


# TODO validate min delta time/value and threshold, check for collision with trips
def preprocess_cycles(connection):
    with connection.cursor(DictCursor) as cursor:
        for nr, imei in enumerate(IMEIS):
            logger.info(__("Preprocessing charging cycles for {}", imei))
            cursor.execute(
                """SELECT Stamp, ChargingCurr, DischargeCurr, BatteryVoltage, soc_smooth FROM imei{imei}
                JOIN webike_sfink.soc ON Stamp = time AND imei = '{imei}'
                WHERE ChargingCurr IS NOT NULL AND ChargingCurr != 0
                ORDER BY Stamp ASC"""
                    .format(imei=imei))
            charge = cursor.fetchall()
            logger.info(__("Preparing the {} rows read from DB for processing", len(charge)))
            smooth(charge, 'ChargingCurr')
            smooth(charge, 'DischargeCurr')

            logger.info("Detecting charging cycles based on current")
            cycles_curr, cycles_curr_disc = extract_cycles_curr(charge)
            logger.info("Detecting charging cycles based on state of charge")
            cycles_soc, cycles_soc_disc = extract_cycles_soc(charge)

            cycles_curr = [(s, e, 'A') for (s, e) in cycles_curr]
            cycles_curr_disc = [(s, e, 'B') for (s, e) in cycles_curr_disc]
            cycles_soc = [(s, e, 'S') for (s, e) in cycles_soc]
            cycles_soc_disc = [(s, e, 'T') for (s, e) in cycles_soc_disc]

            logger.info(__("Writing ({} + {} + {} + {} = {}) detected cycles to DB",
                           len(cycles_curr), len(cycles_curr_disc), len(cycles_soc), len(cycles_soc_disc),
                           len(cycles_curr) + len(cycles_curr_disc) + len(cycles_soc) + len(cycles_soc_disc)))
            for cycle in cycles_curr + cycles_curr_disc + cycles_soc + cycles_soc_disc:
                cursor.execute(
                    """INSERT INTO webike_sfink.charge_cycles (imei, start_time, end_time, type)
                  VALUES (%s, %s, %s, %s);""", [imei, cycle[0]['Stamp'], cycle[1]['Stamp'], cycle[2]])


CYCLE_TYPE_COLORS = {'A': 'm', 'B': '#550055', 'S': 'c', 'T': '#005555'}


def plot_cycles(connection):
    with connection.cursor(DictCursor) as cursor:
        for nr, imei in enumerate(IMEIS):
            logger.info(__("Plotting charging cycles for {}", imei))
            cursor.execute("SELECT * FROM webike_sfink.charge_cycles WHERE imei='{}' ORDER BY start_time".format(imei))
            charge_cycles = cursor.fetchall()

            cursor.execute("SELECT * FROM trip{} ORDER BY start_time ASC".format(imei))
            trips = cursor.fetchall()

            cursor.execute("SELECT MIN(Stamp) as min, MAX(Stamp) as max FROM imei{}".format(imei))
            limits = cursor.fetchone()
            if not limits['min']:
                # FIXME weird MySQL error, non-null column Stamp is null for some tables
                limits['min'] = date(year=2014, month=1, day=1)

            for month in daterange(limits['min'].date(), limits['max'].date() + timedelta(days=1),
                                   relativedelta(months=1)):
                min = month
                max = month + relativedelta(months=1) - timedelta(seconds=1)
                logger.info(__("Plotting {} -- {}-{} from {} to {}", imei, month.year, month.month, min, max))

                cursor.execute(
                    """SELECT Stamp, ChargingCurr, DischargeCurr, soc_smooth FROM imei{imei}
                    JOIN webike_sfink.soc ON Stamp = time AND imei = '{imei}'
                    WHERE Stamp >= '{min}' AND Stamp <= '{max}'
                    ORDER BY Stamp ASC"""
                        .format(imei=imei, min=min, max=max))
                charge_values = cursor.fetchall()
                logger.debug(__("Preparing the {} rows read from DB for plotting", len(charge_values)))
                smooth(charge_values, 'ChargingCurr')
                smooth(charge_values, 'DischargeCurr')

                logger.debug("Graphing data")
                plt.clf()
                plt.xlim(min, max)

                plt.plot(
                    list([x['Stamp'] for x in charge_values]),
                    list([x['soc_smooth'] or -2 for x in charge_values]),
                    'b-', label="State of Charge", alpha=0.9
                )
                plt.plot(
                    list([x['Stamp'] for x in charge_values]),
                    list([x['ChargingCurr_smooth'] / 200 if x['ChargingCurr_smooth'] else -2 for x in charge_values]),
                    'g-', label="Charging Current", alpha=0.9
                )
                plt.plot(
                    list([x['Stamp'] for x in charge_values]),
                    list([discharge_curr_to_ampere(x['DischargeCurr_smooth'])
                          if x['DischargeCurr_smooth'] else -2 for x in charge_values]),
                    'r-', label="Discharge Current", alpha=0.9
                )

                for trip in trips:
                    plt.axvspan(trip['start_time'], trip['end_time'], color='y', alpha=0.5, lw=0)
                for cycle in charge_cycles:
                    plt.axvspan(cycle['start_time'], cycle['end_time'], color=CYCLE_TYPE_COLORS[cycle['type']],
                                alpha=0.5, lw=0)

                handles = list(plt.gca().get_legend_handles_labels()[0])
                handles.append(mpatches.Patch(color='y', label='Trips'))
                handles.append(mpatches.Patch(color='m', label='Charging Cycles (Current based)'))
                handles.append(mpatches.Patch(color='c', label='Charging Cycles (SoC based)'))
                plt.legend(handles=handles, loc='best')

                file = "../out/cc/{}-{}-{}.png".format(imei, month.year, month.month)
                logger.debug(__("Writing graph to {}", file))
                plt.title("{} -- {}-{}".format(imei, month.year, month.month))
                plt.xlim(min, max)
                plt.ylim(-1, 2)
                plt.gcf().set_size_inches(24, 10)
                plt.tight_layout()
                plt.gca().xaxis.set_major_locator(mdates.DayLocator())
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d'))
                plt.savefig(file, dpi=300, bbox_inches='tight')


with DB.connect() as mconnection:
    preprocess_cycles(mconnection)
    mconnection.commit()
    plot_cycles(mconnection)
