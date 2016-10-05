import logging

import scipy as sp
from scipy.optimize import curve_fit

from util.Constants import IMEIS
from util.DB import DictCursor, StreamingDictCursor
from util.Logging import BraceMessage as __
from util.Utils import zip_prev, smooth1, smooth_ignore_missing, progress

__author__ = "Tommy Carpenter, Niko Fink"
logger = logging.getLogger(__name__)

########################################################################################################################
# Constants developed by Tommy Carpenter for his PhD thesis
# see http://hdl.handle.net/10012/9096
# Please note that this approach is only valid during inactivity and the values calculated during trips are
# extremely inaccurate, as the currents vary greatly during usage of the e-bike. (see chapter 8.3.3)
########################################################################################################################

d = {'-20': {}, '-10': {}, '23': {}, '0': {}, '45': {}}

d['-20']['Xs'] = [10 * i for i in
                  [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                   760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                   1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1840, 1840, 1840, 1840]]

d['-10']['Xs'] = [10 * i for i in
                  [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                   760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                   1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1880, 1880, 1880, 1880]]

d['0']['Xs'] = [10 * i for i in
                [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                 760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                 1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1880, 1920, 1920, 1920]]

d['23']['Xs'] = [10 * i for i in
                 [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                  760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                  1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1880, 1920, 1960, 1960]]

d['45']['Xs'] = [10 * i for i in
                 [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                  760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                  1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1880, 1920, 1960, 2000]]

# battery OEM graphs show that max voltage should be 32, but we have to step down the voltage to 8 or so.
offset = 22 / 32

# value 4 (0 indexed) should be close to value 5 for the transition from 1 to 2 to be smooth.
# don't want a discontinuity
d['-20']['Ys'] = [10 * offset * i for i in
                  [2.6, 2.55, 2.6, 2.8, 3.1, 3.15, 3.14375, 3.1374999999999997, 3.13125, 3.125, 3.11875,
                   3.1125, 3.1062499999999997, 3.1, 3.09375, 3.0875, 3.08125, 3.0749999999999997, 3.06875,
                   3.0625, 3.05625, 3.05, 3.0437499999999997, 3.0375, 3.03125, 3.025, 3.01875,
                   3.0124999999999997, 3.00625, 3.0, 2.99375, 2.9875, 2.9812499999999997, 2.975, 2.96875,
                   2.9625, 2.95625, 2.9499999999999997, 2.94375, 2.9375, 2.93125, 2.925, 2.9187499999999997,
                   2.9125, 2.90625, 2.9, 2.7199999999999998, 2.54, 2.36, 2.1799999999999997, 2.0]]

d['-10']['Ys'] = [10 * offset * i for i in
                  [3.2, 3.1, 3.15, 3.2, 3.25, 3.3, 3.29125, 3.2824999999999998, 3.2737499999999997,
                   3.2649999999999997, 3.2562499999999996, 3.2475, 3.23875, 3.23, 3.22125, 3.2125, 3.20375,
                   3.195, 3.18625, 3.1774999999999998, 3.16875, 3.16, 3.15125, 3.1425, 3.13375, 3.125,
                   3.11625, 3.1075, 3.09875, 3.09, 3.08125, 3.0725000000000002, 3.06375, 3.055, 3.04625,
                   3.0375, 3.02875, 3.02, 3.01125, 3.0025, 2.9937500000000004, 2.9850000000000003,
                   2.9762500000000003, 2.9675000000000002, 2.95875, 2.95, 2.8600000000000003, 2.77, 2.68, 2.59, 2.5]]

d['0']['Ys'] = [10 * offset * (i + .2) for i in
                [3.6, 3.55, 3.5, 3.45, 3.416, 3.395, 3.39125, 3.3825, 3.37375, 3.3649999999999998,
                 3.3562499999999997, 3.3474999999999997, 3.33875, 3.33, 3.32125, 3.3125, 3.30375, 3.295,
                 3.28625, 3.2775, 3.26875, 3.26, 3.2512499999999998, 3.2424999999999997, 3.2337499999999997,
                 3.2249999999999996, 3.21625, 3.2075, 3.19875, 3.19, 3.18125, 3.1725, 3.16375, 3.155,
                 3.1462499999999998, 3.1374999999999997, 3.1287499999999997, 3.1199999999999997, 3.11125,
                 3.1025, 3.09375, 3.085, 3.07625, 3.0675, 3.05875, 3.05, 2.94, 2.83, 2.7199999999999998, 2.61, 2.5]]

d['23']['Ys'] = [10 * offset * (i + .2) for i in
                 [3.95, 3.9, 3.85, 3.8, 3.745, 3.7, 3.6862500000000002, 3.6725000000000003, 3.65875, 3.645,
                  3.63125, 3.6175, 3.6037500000000002, 3.5900000000000003, 3.57625, 3.5625, 3.54875, 3.535,
                  3.52125, 3.5075000000000003, 3.49375, 3.48, 3.46625, 3.4525, 3.43875, 3.425, 3.41125,
                  3.3975, 3.38375, 3.37, 3.35625, 3.3425, 3.32875, 3.315, 3.30125, 3.2875, 3.27375, 3.26,
                  3.24625, 3.2325, 3.21875, 3.205, 3.19125, 3.1775, 3.16375, 3.15, 3.02, 2.89, 2.76, 2.63, 2.5]]

d['45']['Ys'] = [10 * offset * (i + .2) for i in
                 [4, 3.965, 3.92, 3.88, 3.84, 3.8, 3.7849999999999997, 3.77, 3.755, 3.7399999999999998,
                  3.7249999999999996, 3.71, 3.695, 3.6799999999999997, 3.665, 3.65, 3.635, 3.62, 3.605, 3.59,
                  3.575, 3.56, 3.545, 3.53, 3.515, 3.5, 3.485, 3.47, 3.455, 3.44, 3.425, 3.41, 3.395, 3.38,
                  3.365, 3.35, 3.335, 3.3200000000000003, 3.305, 3.29, 3.2750000000000004,
                  3.2600000000000002, 3.245, 3.2300000000000004, 3.2150000000000003, 3.2, 3.06, 2.9, 2.7800000000000002,
                  2.64, 2.5]]


def integrate_box(data, i):
    val = data['Xs'][i] * data['Ys'][i]
    return (data['maxwh_box'] - val / 1000) / data['maxwh_box']


def integrate_riemann(data, i):
    i = min(len(vals['riemann_sum']) - 1, i)
    return vals['riemann_sum'][i] / data['maxwh_riemann']


for temp, vals in d.items():
    vals['maxwh_box'] = vals['Xs'][-1] * max(vals['Ys']) / 1000

    vals['riemann_val'] = []
    for y, x1, x2 in zip(vals['Ys'], vals['Xs'], vals['Xs'][1:]):
        vals['riemann_val'].append(y * (x2 - x1))

    vals['riemann_sum'] = []
    last_val = 0
    for val in reversed(vals['riemann_val']):
        last_val += val
        vals['riemann_sum'].insert(0, last_val)

    vals['maxwh_riemann'] = vals['riemann_sum'][0]


def clip(inpt):
    if inpt > 1:
        return 1
    elif inpt < 0:
        return 0
    else:
        return inpt


# linear model code
def model_funcLinear(x, m, b):
    ans = []
    for i in x:
        ans.append(m * i + b)
    return ans


# 3 line model code
def model_func3Line(x, m1, b1, m2, b2, m3, b3):
    ans = []
    for i in range(0, len(x)):
        if i <= 4:
            ans.append(m1 * x[i] + b1)
        elif i <= 46:
            ans.append(m2 * x[i] + b2)
        else:
            ans.append(m3 * x[i] + b3)
    return ans


def model_func2_3Line(x, m1, b1, m2, b2, m3, b3, m):
    if m == 1:
        return m1 * x + b1
    elif m == 2:
        return m2 * x + b2
    else:
        return m3 * x + b3


"""
linear model. ignore first 5 and last 6, corresponding to modes 1 and 3, when training linear model
"""
linearN20, _ = sp.optimize.curve_fit(
    model_funcLinear, d['-20']['Ys'][5:47],
    [integrate_riemann(d['-20'], i) for i in range(5, 47)])
linearN10, _ = sp.optimize.curve_fit(
    model_funcLinear, d['-10']['Ys'][5:47],
    [integrate_riemann(d['-10'], i) for i in range(5, 47)])

"""
three line model
"""
threeLine0, _ = sp.optimize.curve_fit(
    model_func3Line, d['0']['Ys'],
    [integrate_riemann(d['0'], i) for i in range(0, len(d['0']['Xs']))])
threeLineP23, _ = sp.optimize.curve_fit(
    model_func3Line, d['23']['Ys'],
    [integrate_riemann(d['23'], i) for i in range(0, len(d['23']['Xs']))])
threeLineP45, _ = sp.optimize.curve_fit(
    model_func3Line, d['45']['Ys'],
    [integrate_riemann(d['45'], i) for i in range(0, len(d['45']['Xs']))])


########################################################################################################################

def calc_soc(temp, volt):
    """ Calculate the state of charge of one the ebike's batteries from its given temperature and voltage.
    This is a modified version of Tommy's SOCVals, with all unnecessary code thrown out.
    see https://github.com/webike-dev/webike/blob/master/blizzard/SOC.py
    """
    if temp == -20 or temp == -10:
        if temp == -20:
            tl = linearN20
        else:
            assert temp == -10
            tl = linearN10
        (m, b) = tl
        return clip(model_funcLinear([volt], m, b)[0])
    else:
        if temp == 0:
            tl = threeLine0
            y = d['0']['Ys']
        elif temp == 23:
            tl = threeLineP23
            y = d['23']['Ys']
        else:
            assert temp == 45
            tl = threeLineP45
            y = d['45']['Ys']

        (m1, b1, m2, b2, m3, b3) = tl

        if volt >= y[4]:
            return clip(model_func2_3Line(volt, m1, b1, m2, b2, m3, b3, 1))
        elif volt >= y[46]:
            return clip(model_func2_3Line(volt, m1, b1, m2, b2, m3, b3, 2))
        else:
            # Mode 3 is actually invalid. While in mode 3, the voltage drops so rapidly, but the capacity in MaH
            # doesn't, so the equation giving the capacity in wh, which multiplies the two, actually starts INCREASING.
            # To solve this, we will just linearly interpolate to 0 from the starting point of mode 3.
            return clip(model_func2_3Line(y[46], m1, b1, m2, b2, m3, b3, 2))
            # return clip(model_func2_3Line(volt, m1, b1, m2, b2, m3, b3, 3)) #this does not work SOC actually increases


def choose_temp(t):
    return min([-20, -10, 0, 23, 45], key=lambda x: abs(x - t))


def generate_estimate(connection, imei, start, end):
    """ Calculate the state of charge of one the ebike's batteries from its temperature and voltage for the given timespan
    this is a new, simplified implementation of Tommy's grapher.getSOCEstimation
    see https://github.com/webike-dev/webike/blob/master/blizzard/grapher.py
    """
    logger.info(__("Fetching raw SoC data for {} from {} to {}", imei, start, end))
    assert start is not None and end is not None

    with connection.cursor(StreamingDictCursor) as scursor:
        # Select relevant data points
        # This selects one sample from soc before the actual date range,
        # so that the smoothed values are deterministic for further runs
        scursor.execute(
            """(SELECT *
             FROM webike_sfink.soc_rie
             WHERE time < '{start}' AND imei = '{imei}'
             ORDER BY time DESC
             LIMIT 1)
            UNION
            (SELECT
               '{imei}'              AS imei,
               imei.Stamp          AS time,
               imei.BatteryVoltage AS volt,
               soc.volt_smooth,
               imei.TempBattery    AS temp,
               soc.temp_smooth,
               soc.soc,
               soc.soc_smooth
             FROM imei{imei} imei
               LEFT OUTER JOIN webike_sfink.soc_rie soc ON imei.Stamp = soc.time AND soc.imei = '{imei}'
             WHERE Stamp >= '{start}' AND Stamp <= '{end}' AND BatteryVoltage IS NOT NULL AND BatteryVoltage != 0
             ORDER BY Stamp ASC);"""
                .format(imei=imei, start=start, end=end))

        logger.debug("Calculating SoC values")
        insert = []
        rows = progress(scursor.fetchall_unbuffered(), logger=logger,
                        msg="Calculated {countf} samples after {timef}s ({ratef} samples per second)")
        for nr, (prev, cur) in enumerate(zip_prev(rows)):
            if cur['soc_smooth'] is None:
                cur['imei'] = imei
                # Smooth voltage and temperature by 95%
                smooth1(cur, prev, 'volt', default_value=smooth_ignore_missing)
                smooth1(cur, prev, 'temp', default_value=smooth_ignore_missing)
                # Run get_SOC_val on the smoothed values and re-add it to each dictionary
                cur['soc'] = calc_soc(choose_temp(cur['temp_smooth']), cur['volt_smooth'])
                # Smooth SoCs by 95%
                smooth1(cur, prev, 'soc', default_value=smooth_ignore_missing)
                # Queue insert
                insert.append(cur)

        if len(insert) > 0:
            logger.info(__("Inserting {:,} newly calculated samples", len(insert)))
            sql = "INSERT INTO webike_sfink.soc_rie ({}) VALUES ({})" \
                .format(", ".join(cur.keys()), ", ".join(["%s"] * len(cur)))
            rows = [[float(val) if isinstance(val, sp.float64) else val for val in row.values()] for row in insert]
            inserted = scursor.executemany(sql, rows)
            logger.info(__("Inserted {:,} new samples", inserted))


def preprocess_estimates(connection):
    """Make sure that the DB contains SoC information for each recorded sample"""
    logger.info("Preprocessing SoC information for new samples")
    with connection.cursor(DictCursor) as cursor:
        for imei in IMEIS:
            logger.info(__("Checking {} for missing samples", imei))
            # Check if the min/max/count in the samples and soc estimations tables differ
            cursor.execute(
                """SELECT
                  MIN(Stamp)   AS min,
                  MAX(Stamp)   AS max,
                  COUNT(Stamp) AS count
                FROM imei{imei}
                WHERE BatteryVoltage IS NOT NULL AND BatteryVoltage != 0
                UNION ALL
                SELECT
                  MIN(time)   AS min,
                  MAX(time)   AS max,
                  COUNT(time) AS count
                FROM webike_sfink.soc_rie
                WHERE imei = '{imei}'""".format(imei=imei)
            )
            vals = cursor.fetchall()
            # If they are the same, we can assume that each sample has a matching SoC estimation
            if vals[0] == vals[1]:
                logger.info("Got enough SoC values for all samples")
                continue

            # If the min/max/count of the samples and soc estimations differ, we have to find out, which samples
            # are missing their soc estimation.
            # As new, unprocessed samples usually appear in sequence, we are faster if we process all samples
            # from first to last instead of handling each unprocessed sample on its own.
            # This query finds the first and the last unprocessed sample.
            cursor.execute(
                """SELECT MIN(imei.Stamp) AS min, MAX(imei.Stamp) AS max, COUNT(imei.Stamp) AS count
                FROM imei{imei} imei
                  LEFT OUTER JOIN webike_sfink.soc_rie soc ON imei.Stamp = soc.time AND soc.imei = '{imei}'
                WHERE soc.time IS NULL AND imei.BatteryVoltage IS NOT NULL AND imei.BatteryVoltage != 0"""
                    .format(imei=imei)
            )
            vals = cursor.fetchone()
            assert vals['count'] > 0
            logger.info(__("Missing {:,} samples from {} to {}", vals['count'], vals['min'], vals['max']))
            # Generate the estimate for all samples in the found timeframe
            generate_estimate(connection, imei, vals['min'], vals['max'])
