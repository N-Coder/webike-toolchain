import collections
import logging
import os
import pickle
from datetime import timedelta

import matplotlib.pyplot as plt

from util import DB
from util.Constants import IMEIS
from util.DB import DictCursor
from util.Logging import BraceMessage as __

PICKLE_FILE = '../out/charge-{}.pickle'

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")


def extract_cycles_curr(charge_samples):
    cycles = []
    charge_start = charge_end = None
    last_sample = None
    for sample in charge_samples:
        sample['ChargingCurr_smooth'] = .95 * last_sample['ChargingCurr_smooth'] + .05 * sample['ChargingCurr'] \
            if last_sample else sample['ChargingCurr']

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


def curr_to_ampere(val):
    return (val - 504) * 0.033 if val else 0


with DB.connect() as connection:
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
            # plt.figure(nr)
            # plt.plot(
            #     list([x['Stamp'] for x in charge]),
            #     list([x['soc_smooth'] if 'soc_smooth' in x else -2 for x in charge]),
            #     'g-'
            # )
            # plt.plot(
            #     list([x['Stamp'] for x in charge]),
            #     list([x['ChargingCurr_smooth'] / 200 for x in charge]),
            #     'b-'
            # )
            # plt.plot(
            #     list([x['Stamp'] for x in charge]),
            #     list([x['soc_diff'] * 100 for x in charge]),
            #     'r-'
            # )
            #
            # for trip in trips:
            #     plt.axvspan(trip['start_time'], trip['end_time'], color='y', alpha=0.5, lw=0)
            # for cycle in cycles_curr:
            #     plt.axvspan(cycle[0]['Stamp'], cycle[1]['Stamp'], color='c', alpha=0.5, lw=0)
            # for cycle in cycles_soc:
            #     plt.axvspan(cycle[0]['Stamp'], cycle[1]['Stamp'], color='m', alpha=0.5, lw=0)
            # plt.title(imei)


            # TOOD start/end time, duration, Initial/Final State of Charge
plt.show()
