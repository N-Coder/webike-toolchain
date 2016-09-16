import pickle
from datetime import datetime

import matplotlib.pyplot as plt
import pymysql

from soc import SOC
from soc.databaseConnector import *


def choose_temp(t):
    return min([-20, -10, 0, 23, 45], key=lambda x: abs(x - t))


def getSOCEstimation2(cursor, imei, start, end):
    # Select relevant data points
    cursor.execute(
        "SELECT Stamp AS time, BatteryVoltage AS volt, TempBattery AS temp FROM imei{} "
        "WHERE Stamp >= '{}' AND Stamp <= '{}' AND BatteryVoltage IS NOT NULL AND BatteryVoltage != 0 "
        "ORDER BY Stamp".format(imei, start, end))
    socs = cursor.fetchall()

    # Smooth voltage by 95%
    socs[0]['volt_smooth'] = socs[0]['volt']
    for prev, cur in zip(socs, socs[1:]):
        cur['volt_smooth'] = .95 * prev['volt_smooth'] + .05 * cur['volt']

    # Run SOCVals on the list of smoothed values and re-add it to each dictionary
    for soc in socs:
        soc['soc'] = SOC.SOCVal(choose_temp(soc['temp']), soc['volt_smooth'])

    # Smooth SoCs by 95%
    socs[0]['soc_smooth'] = socs[0]['soc']
    for prev, cur in zip(socs, socs[1:]):
        cur['soc_smooth'] = .95 * prev['soc_smooth'] + .05 * cur['soc']

    return socs


def getSOCEstimation(dbc, imei, s, e):
    interpolate_starts = dict()
    interpolate_ends = dict()

    tripStartTimes, tripEndTimes, dists = get_trips(dbc, imei, s, e)

    stmt = "select stamp, batteryVoltage from imei{0} where stamp >= \"{1}\" and stamp <= \"{2}\" and batteryVoltage is not null and batteryVoltage != 0 order by stamp".format(
        imei, s, e)

    Xs = []
    Xlabs = []
    Ys = []

    """there might not be a row where battery voltage is not null at exactly the same time as each trio start and trip end.
    thus, we cant just use the list of tripstarttimes and tripendtimes and find the SOC at exactly those rows.
    Instead, well look at all rows between each (tripstart,tripend) pair, use the earliest with a NONNULL voltage
    as the tripStartSOC, and use the latest as the tripEndSOC."""
    count = 0
    for l in dbc.SQLSelectGenerator(stmt):
        Xs.append(count)
        for j in range(0, len(tripStartTimes)):
            if tripStartTimes[j] <= l[0] <= tripEndTimes[j]:
                if j not in interpolate_starts:
                    interpolate_starts[j] = 1000000000000
                if j not in interpolate_ends:
                    interpolate_ends[j] = -1
                interpolate_starts[j] = min(interpolate_starts[j], count)
                interpolate_ends[j] = max(interpolate_ends[j], count)

        Xlabs.append(l[0])
        Ys.append(float(l[1]))
        count += 1

    if (count == 0):
        return {
            'yAxis': []
        }
    else:

        Ysmoothed = []

        for i in range(0, len(Ys)):
            if i == 0:
                Ysmoothed.append(Ys[i])
            else:
                Ysmoothed.append(.95 * Ysmoothed[i - 1] + .05 * Ys[i - 1])

        # todo
        """TODO: INSTEAD OF ASSUMING 23 DEGREES, QUERY THE BATTERY TEMPERATURE IN THE DATABASE AND USE THE CORRECT CURVE OF
        -20,-10,0,23,45"""

        SOCEstimates = [100 * i for i in SOC.SOCVals(23, Ysmoothed)]

        """now we have to replace the portions of SOCEstimates relating to biking trips.
        this is because battery voltage can fluctuate violently during biking.
        so we will compute the SOC at trip end points and linearly intropolate during periods of biking.

        with respect to the PADDING parameter below, the list of "tripendtimes" is not EXACTLY accurate. these
        can be up to five minutes prior to when the bike was actually at rest. so we will actually compute the SOC
        at 5 minutes past each trip and and linearly intropolate in there.

        We do the same for trip starts. So we actually interpiolate in the range(5 mins before bike start, 5 mins after bike start) for each trip."""
        PADDING = 180
        data = []
        for i in range(0, len(SOCEstimates)):
            for k in sorted(interpolate_starts.keys()):
                if i == interpolate_starts[k]:
                    # this will bomb if a trip starts within 5 minutes of midnight... fixlater
                    SOC_Start = SOCEstimates[i - PADDING]
                    SOC_End = SOCEstimates[interpolate_ends[k] + PADDING]
                    delta = (SOC_Start - SOC_End) / (interpolate_ends[k] - interpolate_starts[k] + PADDING + PADDING)
                    for n in range(i - PADDING, interpolate_ends[k] + PADDING):
                        SOCEstimates[n] = SOC_Start - delta * (n - i + PADDING)
                    i = interpolate_ends[k]  # sklip to this point to save time
            data.append([Xlabs[i], SOCEstimates[i]])
    return {
        'yAxis': data
    }


def get_trips(dbc, imei, curDate, end):
    tripStartTimes, tripEndTimes, dists = [], [], []
    query = "select * from trip{0} where start_time >= \"{1}\" and start_time <= \"{2}\" order by start_time".format(
        imei, curDate, end)
    for record in dbc.SQLSelectGenerator(query):
        tripStartTimes.append(record[1])
        tripEndTimes.append(record[2])
        if record[3] is not None and float(record[3]) != 0:  # distance
            dists.append(float(record[3]))
        else:
            dists.append(0)
    return tripStartTimes, tripEndTimes, dists


connection = pymysql.connect(
    host="tornado.cs.uwaterloo.ca",
    port=3306,
    user=os.environ['MYSQL_USER'],
    passwd=os.environ['MYSQL_PASSWORD'],
    db="webike"
)
cursor = connection.cursor(pymysql.cursors.DictCursor)

imei = 5233
start = datetime(year=2015, month=1, day=1)
end = datetime(year=2016, month=9, day=30)
# soc = getSOCEstimation2(cursor, imei, start, end)
# pickle.dump(soc, open("soc.p", "wb"))
# soc2 = getSOCEstimation(databaseConnector(), imei, start, end)
# pickle.dump(soc2, open("soc2.p", "wb"))

soc = pickle.load(open("soc.p", "rb"))
print('unpickled1')
soc2 = pickle.load(open("soc2.p", "rb"))
print('unpickled2')
plt.plot(
    list(map(lambda x: x['time'], soc)),
    list(map(lambda x: x['soc'] * 100, soc)),
    'b-'
)
plt.plot(
    list(map(lambda x: x['time'], soc)),
    list(map(lambda x: x['soc_smooth'] * 100, soc)),
    'r-'
)
plt.plot(
    list(map(lambda x: x[0], soc2['yAxis'])),
    list(map(lambda x: x[1], soc2['yAxis'])),
    'g-'
)

cursor.execute(
    "SELECT  start_time, end_time FROM trip{} WHERE start_time >= '{}' AND end_time <= '{}' ORDER BY start_time"
        .format(imei, start, end))
trips = cursor.fetchall()
for trip in trips:
    plt.axvspan(trip['start_time'], trip['end_time'], color='y', alpha=0.5, lw=0)
plt.show()

connection.close()
