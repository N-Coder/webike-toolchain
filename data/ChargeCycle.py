import logging
from datetime import timedelta, datetime

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


def daterange(start, stop=datetime.now(), step=timedelta(days=1)):
    """Similar to :py:func:`builtins.range`, but for dates"""
    if start < stop:
        cmp = lambda a, b: a < b
        inc = lambda a: a + step
    else:
        cmp = lambda a, b: a > b
        inc = lambda a: a - step
    yield start
    start = inc(start)
    while cmp(start, stop):
        yield start
        start = inc(start)


def extract_cycles_curr(charge_samples,
                        charge_thresh_start=50, charge_thresh_end=50, min_charge_samples=100,
                        max_sample_delay=timedelta(minutes=10), min_charge_time=timedelta(minutes=10)):
    """Detect charging cycles based on the ChargingCurr."""
    cycles = []
    discarded_cycles = []
    charge_start = charge_end = None
    charge_sample_count = 0
    charge_avg_curr = 0

    last_sample = None
    for sample in charge_samples:
        # did charging start?
        if not charge_start:
            if sample['ChargingCurr'] > charge_thresh_start:
                # yes, because ChargingCurr is high
                charge_start = sample
                charge_sample_count = 1
                charge_avg_curr = sample['ChargingCurr']

        # did charging stop?
        else:
            if sample['ChargingCurr'] < charge_thresh_end:
                # yes, because ChargingCurr is back to normal
                charge_end = last_sample
            elif sample['Stamp'] - last_sample['Stamp'] > max_sample_delay:
                # yes, because we didn't get a sample for the last few mins
                charge_end = last_sample
            else:
                # nope, continue counting
                charge_sample_count += 1
                charge_avg_curr = (charge_avg_curr + sample['ChargingCurr']) / 2

            if charge_end:
                cycle = (charge_start, charge_end, charge_sample_count, charge_avg_curr)
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
                charge_avg_curr = 0
        last_sample = sample
    return cycles, discarded_cycles


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

            logger.info("Detecting charging cycles based on current")
            cycles_curr, cycles_curr_disc = extract_cycles_curr(charge)

            logger.info(__("Writing {} detected cycles to DB", len(cycles_curr)))
            for cycle in cycles_curr:
                cursor.execute(
                    """INSERT INTO webike_sfink.charge_cycles
                    (imei, start_time, end_time, sample_count, avg_thresh_val, type)
                    VALUES (%s, %s, %s, %s, %s, %s);""",
                    [imei, cycle[0]['Stamp'], cycle[1]['Stamp'], cycle[2], cycle[3], 'A'])


def plot_cycles_timeline(connection):
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
                limits['min'] = datetime(year=2014, month=1, day=1)

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

                logger.debug(__("Generating graph from {} rows", len(charge_values)))
                plt.clf()
                plt.xlim(min, max)

                plt.plot(
                    list([x['Stamp'] for x in charge_values]),
                    list([x['soc_smooth'] or -2 for x in charge_values]),
                    'b-', label="State of Charge", alpha=0.9
                )
                plt.plot(
                    list([x['Stamp'] for x in charge_values]),
                    list([x['ChargingCurr'] / 200 if x['ChargingCurr'] else -2 for x in charge_values]),
                    'r-', label="Charging Current", alpha=0.9
                )

                for trip in trips:
                    plt.axvspan(trip['start_time'], trip['end_time'], color='y', alpha=0.5, lw=0)
                for cycle in charge_cycles:
                    plt.axvspan(cycle['start_time'], cycle['end_time'], color='m', alpha=0.5, lw=0)

                handles = list(plt.gca().get_legend_handles_labels()[0])
                handles.append(mpatches.Patch(color='y', label='Trips'))
                handles.append(mpatches.Patch(color='m', label='Charging Cycles (Current based)'))
                plt.legend(handles=handles, loc='best')

                file = "../out/cc/{}-{}-{}.png".format(imei, month.year, month.month)
                logger.debug(__("Writing graph to {}", file))
                plt.title("{} -- {}-{}".format(imei, month.year, month.month))
                plt.xlim(min, max)
                plt.ylim(-3, 5)
                plt.gcf().set_size_inches(24, 10)
                plt.tight_layout()
                plt.gca().xaxis.set_major_locator(mdates.DayLocator())
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d'))
                plt.savefig(file, dpi=300, bbox_inches='tight')


# TODO start/end time, duration, Initial/Final State of Charge

with DB.connect() as mconnection:
    preprocess_cycles(mconnection)
    mconnection.commit()
    plot_cycles_timeline(mconnection)
