from datetime import datetime

from soc import SOC
from soc.databaseConnector import *


def getSOCEstimation(dbc, imei, sMonth, sDay, sYear):
    startDate = datetime(sYear, sMonth, sDay)
    s = startDate.strftime('%y-%m-%d') + " 00:00:00"
    e = startDate.strftime('%y-%m-%d') + " 23:59:59"

    interpolate_starts = dict()
    interpolate_ends = dict()

    tripStartTimes, tripEndTimes, dists = get_trips(dbc, imei, s, e)

    stmt = "select stamp, batteryVoltage from imei{0} where stamp >= \"{1}\" and stamp <= \"{2}\" and batteryVoltage is not null and batteryVoltage != 0 order by stamp".format(
        imei, s, e)

    Xs = []
    Xdatetimes = []
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

        Xlabs.append(l[0].timestamp() * 1000)
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
        if record[3] is not None and float(record[3]) != 0:
            dists.append(float(record[3]))
        else:
            dists.append(0)
    return tripStartTimes, tripEndTimes, dists


dbc = databaseConnector()

print(getSOCEstimation(dbc, 5233, 9, 15, 2016))

dbc.shutDown()
