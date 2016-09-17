from datetime import datetime, timedelta

import pymysql
import scipy as sp
from scipy.optimize import curve_fit

from Constants import IMEIS

d = {'-20': {}, '-10': {}, '23': {}, '0': {}, '45': {}}

d["-20"]["Xs"] = [10 * i for i in
                  [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                   760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                   1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1840, 1840, 1840, 1840]]

d["-10"]["Xs"] = [10 * i for i in
                  [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                   760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                   1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1880, 1880, 1880, 1880]]

d["0"]["Xs"] = [10 * i for i in
                [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                 760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                 1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1880, 1920, 1920, 1920]]

d["23"]["Xs"] = [10 * i for i in
                 [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                  760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                  1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1880, 1920, 1960, 1960]]

d["45"]["Xs"] = [10 * i for i in
                 [0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400, 440, 480, 520, 560, 600, 640, 680, 720,
                  760, 800, 840, 880, 920, 960, 1000, 1040, 1080, 1120, 1160, 1200, 1240, 1280, 1320, 1360,
                  1400, 1440, 1480, 1520, 1560, 1600, 1640, 1680, 1720, 1760, 1800, 1840, 1880, 1920, 1960, 2000]]

# battery OEM graphs show that max voltage should be 32, but we have tos tep down the voltage to 8 or so.
offset = 22 / 32

# value 4 (0 indexed) should be close to value 5 for the transition from 1 to 2 to be smooth.
# don't want a discontinuity
d["-20"]["Ys"] = [10 * offset * i for i in
                  [2.6, 2.55, 2.6, 2.8, 3.1, 3.15, 3.14375, 3.1374999999999997, 3.13125, 3.125, 3.11875,
                   3.1125, 3.1062499999999997, 3.1, 3.09375, 3.0875, 3.08125, 3.0749999999999997, 3.06875,
                   3.0625, 3.05625, 3.05, 3.0437499999999997, 3.0375, 3.03125, 3.025, 3.01875,
                   3.0124999999999997, 3.00625, 3.0, 2.99375, 2.9875, 2.9812499999999997, 2.975, 2.96875,
                   2.9625, 2.95625, 2.9499999999999997, 2.94375, 2.9375, 2.93125, 2.925, 2.9187499999999997,
                   2.9125, 2.90625, 2.9, 2.7199999999999998, 2.54, 2.36, 2.1799999999999997, 2.0]]

d["-10"]["Ys"] = [10 * offset * i for i in
                  [3.2, 3.1, 3.15, 3.2, 3.25, 3.3, 3.29125, 3.2824999999999998, 3.2737499999999997,
                   3.2649999999999997, 3.2562499999999996, 3.2475, 3.23875, 3.23, 3.22125, 3.2125, 3.20375,
                   3.195, 3.18625, 3.1774999999999998, 3.16875, 3.16, 3.15125, 3.1425, 3.13375, 3.125,
                   3.11625, 3.1075, 3.09875, 3.09, 3.08125, 3.0725000000000002, 3.06375, 3.055, 3.04625,
                   3.0375, 3.02875, 3.02, 3.01125, 3.0025, 2.9937500000000004, 2.9850000000000003,
                   2.9762500000000003, 2.9675000000000002, 2.95875, 2.95, 2.8600000000000003, 2.77, 2.68, 2.59, 2.5]]

d["0"]["Ys"] = [10 * offset * (i + .2) for i in
                [3.6, 3.55, 3.5, 3.45, 3.416, 3.395, 3.39125, 3.3825, 3.37375, 3.3649999999999998,
                 3.3562499999999997, 3.3474999999999997, 3.33875, 3.33, 3.32125, 3.3125, 3.30375, 3.295,
                 3.28625, 3.2775, 3.26875, 3.26, 3.2512499999999998, 3.2424999999999997, 3.2337499999999997,
                 3.2249999999999996, 3.21625, 3.2075, 3.19875, 3.19, 3.18125, 3.1725, 3.16375, 3.155,
                 3.1462499999999998, 3.1374999999999997, 3.1287499999999997, 3.1199999999999997, 3.11125,
                 3.1025, 3.09375, 3.085, 3.07625, 3.0675, 3.05875, 3.05, 2.94, 2.83, 2.7199999999999998, 2.61, 2.5]]

d["23"]["Ys"] = [10 * offset * (i + .2) for i in
                 [3.95, 3.9, 3.85, 3.8, 3.745, 3.7, 3.6862500000000002, 3.6725000000000003, 3.65875, 3.645,
                  3.63125, 3.6175, 3.6037500000000002, 3.5900000000000003, 3.57625, 3.5625, 3.54875, 3.535,
                  3.52125, 3.5075000000000003, 3.49375, 3.48, 3.46625, 3.4525, 3.43875, 3.425, 3.41125,
                  3.3975, 3.38375, 3.37, 3.35625, 3.3425, 3.32875, 3.315, 3.30125, 3.2875, 3.27375, 3.26,
                  3.24625, 3.2325, 3.21875, 3.205, 3.19125, 3.1775, 3.16375, 3.15, 3.02, 2.89, 2.76, 2.63, 2.5]]

d["45"]["Ys"] = [10 * offset * (i + .2) for i in
                 [4, 3.965, 3.92, 3.88, 3.84, 3.8, 3.7849999999999997, 3.77, 3.755, 3.7399999999999998,
                  3.7249999999999996, 3.71, 3.695, 3.6799999999999997, 3.665, 3.65, 3.635, 3.62, 3.605, 3.59,
                  3.575, 3.56, 3.545, 3.53, 3.515, 3.5, 3.485, 3.47, 3.455, 3.44, 3.425, 3.41, 3.395, 3.38,
                  3.365, 3.35, 3.335, 3.3200000000000003, 3.305, 3.29, 3.2750000000000004,
                  3.2600000000000002, 3.245, 3.2300000000000004, 3.2150000000000003, 3.2, 3.06, 2.9, 2.7800000000000002,
                  2.64, 2.5]]

d["-20"]["maxwh"] = d["-20"]["Xs"][-1] * max(d["-20"]["Ys"]) / 1000
d["-10"]["maxwh"] = d["-10"]["Xs"][-1] * max(d["-10"]["Ys"]) / 1000
d["0"]["maxwh"] = d["0"]["Xs"][-1] * max(d["0"]["Ys"]) / 1000
d["23"]["maxwh"] = d["23"]["Xs"][-1] * max(d["23"]["Ys"]) / 1000
d["45"]["maxwh"] = d["45"]["Xs"][-1] * max(d["45"]["Ys"]) / 1000


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
linearN20, parm_cov = sp.optimize.curve_fit(
    model_funcLinear, d["-20"]["Ys"][5:47],
    [(d["-20"]["maxwh"] - d["-20"]["Xs"][i] * d["-20"]["Ys"][i] / 1000) / d["-20"]["maxwh"]
     for i in range(5, 47)])
linearN10, parm_cov = sp.optimize.curve_fit(
    model_funcLinear, d["-10"]["Ys"][5:47],
    [(d["-10"]["maxwh"] - d["-10"]["Xs"][i] * d["-10"]["Ys"][i] / 1000) / d["-10"]["maxwh"]
     for i in range(5, 47)])

"""
three line model
"""
threeLine0, parm_cov = sp.optimize.curve_fit(
    model_func3Line, d["0"]["Ys"],
    [(d["0"]["maxwh"] - d["0"]["Xs"][i] * d["0"]["Ys"][i] / 1000) / d["0"]["maxwh"]
     for i in range(0, len(d["0"]["Xs"]))])
threeLineP23, parm_cov = sp.optimize.curve_fit(
    model_func3Line, d["23"]["Ys"],
    [(d["23"]["maxwh"] - d["23"]["Xs"][i] * d["23"]["Ys"][i] / 1000) / d["23"]["maxwh"]
     for i in range(0, len(d["23"]["Xs"]))])
threeLineP45, parm_cov = sp.optimize.curve_fit(
    model_func3Line, d["45"]["Ys"],
    [(d["45"]["maxwh"] - d["45"]["Xs"][i] * d["45"]["Ys"][i] / 1000) / d["45"]["maxwh"]
     for i in range(0, len(d["45"]["Xs"]))])


# this is a modified version of Tommy's SOCVals, with all unnecessary code thrown out
def get_SOC_val(temp, volt):
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
            y = d["0"]["Ys"]
        elif temp == 23:
            tl = threeLineP23
            y = d["23"]["Ys"]
        else:
            assert temp == 45
            tl = threeLineP45
            y = d["45"]["Ys"]

        (m1, b1, m2, b2, m3, b3) = tl

        if volt >= y[4]:
            return clip(model_func2_3Line(volt, m1, b1, m2, b2, m3, b3, 1))
        elif volt >= y[46]:
            return clip(model_func2_3Line(volt, m1, b1, m2, b2, m3, b3, 2))
        else:
            """Mode 3 is actually invalid. While in mode 3, the voltage drops so rapidly, buy the capacity in MaH doesn't,
            so the equation giving the capacity in wh, which multiplies the two, actually starts INCREASING.
            To solve this, we will just linearly interpolate to 0 from the starting point of mode 3. """
            return clip(model_func2_3Line(y[46], m1, b1, m2, b2, m3, b3, 2))
            # return clip(model_func2_3Line(volt, m1, b1, m2, b2, m3, b3, 3)) #this does not work SOC actually increases


def choose_temp(t):
    return min([-20, -10, 0, 23, 45], key=lambda x: abs(x - t))


# this is a new, simplified implementation of Tommy's grapher.getSOCEstimation
def get_SOC_estimation(connection, imei, start, end):
    print('Generating SoC estimation for {} from {} to {}'.format(imei, start, end))

    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        # Select relevant data points
        count = cursor.execute(
            "SELECT Stamp AS time, BatteryVoltage AS volt, TempBattery AS temp FROM imei{} "
            "WHERE Stamp >= '{}' AND Stamp <= '{}' AND BatteryVoltage IS NOT NULL AND BatteryVoltage != 0 "
            "ORDER BY Stamp".format(imei, start, end))
        print('Got {} samples'.format(count))
        if count < 1: return []
        socs = cursor.fetchall()

        # Calculate first sample, all further samples will be smoothed
        socs[0]['volt_smooth'] = socs[0]['volt']
        socs[0]['temp_smooth'] = socs[0]['temp']
        socs[0]['soc'] = get_SOC_val(choose_temp(socs[0]['temp_smooth']), socs[0]['volt_smooth'])
        socs[0]['soc_smooth'] = socs[0]['soc']

        last_print = datetime.now()
        for nr, (prev, cur) in enumerate(zip(socs, socs[1:])):
            if (datetime.now() - last_print).total_seconds() > 5:
                print("\t{:.0%} complete".format(nr / count))
                last_print = datetime.now()

            # Smooth voltage and temperature by 95%
            cur['volt_smooth'] = .95 * prev['volt_smooth'] + .05 * cur['volt']
            cur['temp_smooth'] = .95 * prev['temp_smooth'] + .05 * cur['temp']
            # Run get_SOC_val on the smoothed values and re-add it to each dictionary
            cur['soc'] = get_SOC_val(choose_temp(cur['temp_smooth']), cur['volt_smooth'])
            # Smooth SoCs by 95%
            cur['soc_smooth'] = .95 * prev['soc_smooth'] + .05 * cur['soc']

            sql = "INSERT INTO webike_sfink.soc ({}) VALUES ({}) ON DUPLICATE KEY UPDATE time=time" \
                .format(", ".join(cur.keys()), ", ".join(["%s"] * len(cur)))
            cursor.execute(sql, [float(val) if isinstance(val, sp.float64) else val for val in cur.values()])

        connection.commit()
        return socs
    except:
        connection.rollback()
        raise


if __name__ == "__main__":
    from DB import connection

    for imei in IMEIS:
        print('Processing IMEI ' + imei)
        socs = get_SOC_estimation(connection, imei, datetime.min, datetime.now() + timedelta(days=1))

    connection.close()
