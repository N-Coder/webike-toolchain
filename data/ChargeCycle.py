import collections
import logging
import os
import pickle
from datetime import timedelta, datetime

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta

from util import DB
from util.Constants import IMEIS
from util.DB import DictCursor
from util.Logging import BraceMessage as __

PICKLE_FILE = '../out/charge-{}.pickle'

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")


# TODO start/end time, duration, Initial/Final State of Charge


def daterange(start, stop=datetime.now(), step=timedelta(days=1)):
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


def smooth(samples, label):
    label_smooth = label + '_smooth'
    last_sample = None
    for sample in samples:
        if not (sample and label in sample and sample[label]):
            if not (last_sample and label_smooth in last_sample and
                        last_sample[label_smooth]):
                sample[label_smooth] = None
            else:
                sample[label_smooth] = last_sample[label_smooth]
        else:
            if not (last_sample and label_smooth in last_sample and
                        last_sample[label_smooth]):
                sample[label_smooth] = sample[label]
            else:
                sample[label_smooth] = .95 * last_sample[label_smooth] \
                                       + .05 * sample[label]
        last_sample = sample


def curr_to_ampere(val):
    return (val - 504) * 0.033 if val else 0


def extract_cycles_curr(charge_samples):
    cycles = []
    charge_start = charge_end = None
    last_sample = None
    for sample in charge_samples:
        if not charge_start:
            if sample['ChargingCurr_smooth'] > 50:
                charge_start = sample

        else:
            if sample['ChargingCurr_smooth'] < 50:
                charge_end = sample
            elif sample['Stamp'] - last_sample['Stamp'] > timedelta(minutes=10):
                charge_end = last_sample

            if charge_end:
                if charge_end['Stamp'] - charge_start['Stamp'] > timedelta(minutes=10):
                    cycles.append((charge_start, charge_end))
                charge_start = None
                charge_end = None
        last_sample = sample
    return cycles


def extract_cycles_soc(charge_samples):
    cycles = []
    soc_history = collections.deque(maxlen=10)
    charge_start = charge_end = None
    last_sample = None
    for sample in charge_samples:
        soc_history.append(sample['soc_smooth'])
        l = len(soc_history)
        if l >= soc_history.maxlen:
            h = list(soc_history)
            new_avg = sum(h[l // 2:l]) / len(h[l // 2:l])
            old_avg = sum(h[0:l // 2]) / len(h[0:l // 2])
            sample['soc_diff'] = new_avg - old_avg
        else:
            sample['soc_diff'] = 0

        if not charge_start:
            if sample['soc_diff'] > 0.001:
                charge_start = sample

        else:
            if sample['soc_diff'] < 0.001:
                charge_end = sample
            elif sample['Stamp'] - last_sample['Stamp'] > timedelta(minutes=10):
                charge_end = last_sample

            if charge_end:
                if charge_end['Stamp'] - charge_start['Stamp'] > timedelta(minutes=30):
                    cycles.append((charge_start, charge_end))
                charge_start = None
                charge_end = None
        last_sample = sample
    return cycles


def preprocess_data(connection):
    with connection.cursor(DictCursor) as cursor:
        logger.info("Preprocessing charging cycles")

        for nr, imei in enumerate(IMEIS):
            logger.info(__("Preprocessing charging cycles for {}", imei))
            if not os.path.exists(PICKLE_FILE.format(imei)):
                print("regenerate")
                cursor.execute(
                    """SELECT Stamp, ChargingCurr, DischargeCurr, BatteryVoltage, soc_smooth FROM imei{imei}
                    JOIN webike_sfink.soc ON Stamp = time AND imei = '{imei}'
                    WHERE ChargingCurr IS NOT NULL AND ChargingCurr != 0
                    ORDER BY Stamp ASC"""
                        .format(imei=imei))
                charge = cursor.fetchall()
                smooth(charge, 'ChargingCurr')
                smooth(charge, 'DischargeCurr')
                logger.info(__("{} rows read from DB", len(charge)))

                cursor.execute(
                    "SELECT  start_time, end_time FROM trip{} ORDER BY start_time ASC"
                        .format(imei))
                trips = cursor.fetchall()

                # with open(PICKLE_FILE.format(imei), 'wb') as f:
                #     pickle.dump((charge, trips), f)
            else:
                print("reuse")
                with open(PICKLE_FILE.format(imei), 'rb') as f:
                    (charge, trips) = pickle.load(f)

            cycles_curr = extract_cycles_curr(charge)
            cycles_soc = extract_cycles_soc(charge)

            for cycle in cycles_curr:
                cursor.execute(
                    """INSERT INTO webike_sfink.charge_cycles (imei, start_time, end_time, type)
                  VALUES (%s, %s, %s, %s);""", imei, cycle[0]['Stamp'], cycle[1]['Stamp'], 'A')

            for cycle in cycles_soc:
                cursor.execute(
                    """INSERT INTO webike_sfink.charge_cycles (imei, start_time, end_time, type)
                  VALUES (%s, %s, %s, %s);""", imei, cycle[0]['Stamp'], cycle[1]['Stamp'], 'S')

            connection.commit()


def plot_cycles(connection):
    with connection.cursor(DictCursor) as cursor:
        for nr, imei in enumerate(IMEIS[0:1]):
            cursor.execute("SELECT * FROM webike_sfink.charge_cycles WHERE imei='{}' ORDER BY start_time".format(imei))
            charge_cycles = cursor.fetchall()

            cursor.execute("SELECT * FROM trip{} ORDER BY start_time ASC".format(imei))
            trips = cursor.fetchall()

            cursor.execute("SELECT MIN(Stamp) as min, MAX(Stamp) as max FROM imei{}".format(imei))
            limits = cursor.fetchone()

            for month in daterange(limits['min'].date(), limits['max'].date() + timedelta(days=1),
                                   relativedelta(months=1)):
                min = month
                max = month + relativedelta(months=1) - timedelta(seconds=1)
                print("Plotting {} -- {}-{} from {} to {}".format(imei, month.year, month.month, min, max))

                cursor.execute(
                    """SELECT Stamp, ChargingCurr, DischargeCurr, soc_smooth FROM imei{imei}
                    JOIN webike_sfink.soc ON Stamp = time AND imei = '{imei}'
                    WHERE Stamp >= '{min}' AND Stamp <= '{max}'
                    ORDER BY Stamp ASC"""
                        .format(imei=imei, min=min, max=max))
                charge_values = cursor.fetchall()
                smooth(charge_values, 'ChargingCurr')
                smooth(charge_values, 'DischargeCurr')

                plt.clf()
                plt.xlim(min, max)

                plt.plot(
                    list([x['Stamp'] for x in charge_values]),
                    list([x['soc_smooth'] or -2 for x in charge_values]),
                    'b-', label="State of Charge"
                )
                plt.plot(
                    list([x['Stamp'] for x in charge_values]),
                    list([x['ChargingCurr_smooth'] / 200 if x['ChargingCurr_smooth'] else -2 for x in charge_values]),
                    'g-', label="Charging Current"
                )
                plt.plot(
                    list([x['Stamp'] for x in charge_values]),
                    list([curr_to_ampere(x['DischargeCurr_smooth'])
                          if x['DischargeCurr_smooth'] else -2 for x in charge_values]),
                    'r-', label="Discharge Current"
                )

                for trip in trips:
                    plt.axvspan(trip['start_time'], trip['end_time'], color='y', alpha=0.5, lw=0)
                for cycle in charge_cycles:
                    color = 'm' if cycle['type'] == 'A' else 'c'
                    plt.axvspan(cycle['start_time'], cycle['end_time'], color=color, alpha=0.5, lw=0)

                handles = list(plt.gca().get_legend_handles_labels()[0])
                handles.append(mpatches.Patch(color='y', label='Trips'))
                handles.append(mpatches.Patch(color='m', label='Charging Cycles (Current based)'))
                handles.append(mpatches.Patch(color='c', label='Charging Cycles (SoC based)'))
                plt.legend(handles=handles, loc='lower left')

                plt.title("{} -- {}-{}".format(imei, month.year, month.month))
                plt.xlim(min, max)
                plt.gcf().set_size_inches(24, 10)
                plt.tight_layout()
                plt.gca().xaxis.set_major_locator(mdates.DayLocator())
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d'))
                plt.savefig("../out/cc/{}-{}-{}.png".format(imei, month.year, month.month), dpi=300,
                            bbox_inches='tight')


with DB.connect() as mconnection:
    plot_cycles(mconnection)
