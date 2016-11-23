import copy
import logging
from datetime import timedelta

import matplotlib.pyplot as plt
import numpy as np
from iss4e.db.mysql import DictCursor, QualifiedDictCursor
from iss4e.util import BraceMessage as __
from iss4e.util import progress
from matplotlib.ticker import MaxNLocator
from webike.data import WeatherGC
from webike.data import WeatherWU
from webike.util.constants import IMEIS
from webike.util.plot import order_hists, to_hour_bin, hist_day_hours, hist_year_months, hist_week_days, \
    hist_duration_minutes

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)

HIST_DATA = {'start_times': [], 'start_weekday': [], 'start_month': [], 'distances': [], 'durations': [],
             'initial_soc': [], 'final_soc': [], 'trip_temp': [],
             'trip_weather': copy.deepcopy(WeatherGC.HIST_DATA), 'trip_metar': copy.deepcopy(WeatherWU.HIST_DATA)}


def preprocess_trips(connection):
    logger.info("Preprocessing JOIN information for new trips")
    with connection.cursor(DictCursor) as cursor:
        for imei in IMEIS:
            cursor.execute("SELECT trip{imei}.* FROM trip{imei} LEFT JOIN webike_sfink.trips ON "
                           "trip{imei}.id = trips.trip AND trips.imei='{imei}' WHERE trips.trip IS NULL;"
                           .format(imei=imei))
            unprocessed_trips = cursor.fetchall()
            logger.info(__("Processing {} new entries for IMEI {}", len(unprocessed_trips), imei))
            for nr, trip in enumerate(unprocessed_trips):
                logger.info(__("{} of {}: processing new trip {}#{}",
                               nr + 1, len(unprocessed_trips), imei, trip['id']))

                cursor.execute(
                    "SELECT datetime, ABS(TIMESTAMPDIFF(SECOND, datetime, '{}')) AS diff "
                    "FROM webike_sfink.weather ORDER BY diff LIMIT 1"
                        .format(trip['start_time']))
                weather_sample = cursor.fetchone()
                cursor.execute(
                    "SELECT stamp, ABS(TIMESTAMPDIFF(SECOND, stamp, '{}')) AS diff "
                    "FROM webike_sfink.weather_metar ORDER BY diff LIMIT 1"
                        .format(trip['start_time']))
                metar_sample = cursor.fetchone()

                cursor.execute(
                    "SELECT AVG(TempBox) AS avg_temp FROM imei{} "
                    "WHERE Stamp >= '{}' + INTERVAL 5 MINUTE AND Stamp <= '{}'"
                        .format(imei, trip['start_time'], trip['end_time']))
                avg_temp = cursor.fetchone()

                res = cursor.execute(
                    "INSERT INTO webike_sfink.trips(imei, trip, start_time, end_time, distance, weather, metar, avg_temp) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s)",
                    (imei, trip['id'], trip['start_time'], trip['end_time'], trip['distance'],
                     weather_sample['datetime'], metar_sample['stamp'], avg_temp['avg_temp']))
                if res != 1:
                    raise AssertionError("Illegal result {} for row #{}: {}".format(res, nr, trip))


def extract_hist(connection):
    logger.info("Generating trip histogram data")

    with connection.cursor(QualifiedDictCursor) as qcursor:
        hist_data = copy.deepcopy(HIST_DATA)

        for imei in IMEIS:
            logger.info(__("Processing IMEI {}", imei))

            qcursor.execute(
                "SELECT * "
                "FROM webike_sfink.trips trip "
                "  LEFT OUTER JOIN imei{imei} first_sample ON first_sample.Stamp = trip.start_time "
                "  LEFT OUTER JOIN imei{imei} last_sample ON last_sample.Stamp = trip.end_time "
                "  LEFT OUTER JOIN webike_sfink.soc first_soc ON first_soc.time = trip.start_time"
                "                                            AND first_soc.imei = '{imei}' "
                "  LEFT OUTER JOIN webike_sfink.soc last_soc ON last_soc.time = trip.end_time"
                "                                           AND last_soc.imei = '{imei}' "
                "  LEFT OUTER JOIN webike_sfink.weather weather ON trip.weather = weather.datetime "
                "  LEFT OUTER JOIN webike_sfink.weather_metar metar ON trip.metar = metar.stamp "
                "WHERE trip.imei = '{imei}'".format(imei=imei))
            trips = qcursor.fetchall()
            for trip in progress(trips):
                hist_data['start_times'].append(to_hour_bin(trip['first_sample.Stamp']))
                hist_data['start_weekday'].append(trip['first_sample.Stamp'].weekday())
                hist_data['start_month'].append(trip['first_sample.Stamp'].month)
                hist_data['durations'].append(
                    (trip['last_sample.Stamp'] - trip['first_sample.Stamp']) / timedelta(minutes=1))

                if trip['trip.avg_temp'] is not None:
                    hist_data['trip_temp'].append(float(trip['trip.avg_temp']))

                if trip['trip.distance'] is not None:
                    hist_data['distances'].append(float(trip['trip.distance']))

                if trip['first_soc.soc_smooth'] is not None:
                    hist_data['initial_soc'].append(float(trip['first_soc.soc_smooth']))

                if trip['last_soc.soc_smooth'] is not None:
                    hist_data['final_soc'].append(float(trip['last_soc.soc_smooth']))

                weather_prefix = 'weather.'
                for key, val in trip.items():
                    if key.startswith(weather_prefix):
                        WeatherGC.append_hist(hist_data['trip_weather'], key[len(weather_prefix):], val)

                WeatherWU.append_hist(hist_data['trip_metar'], trip['metar.metar'], trip['metar.stamp'])
        return hist_data


def plot_trips(hist_data):
    logger.info("Plotting trip graphs")
    plt.clf()
    hist_day_hours(plt.gca(), hist_data['start_times'])
    plt.xlabel("Time of Day")
    plt.ylabel("Number of Trips")
    plt.title("Number of Trips per Hour of Day")
    plt.tight_layout()
    plt.savefig("out/trips_per_hour.png")

    plt.clf()
    hist_week_days(plt.gca(), hist_data['start_weekday'])
    plt.xlabel("Weekday")
    plt.ylabel("Number of Trips")
    plt.title("Number of Trips per Weekday")
    plt.tight_layout()
    plt.savefig("out/trips_per_weekday.png")

    plt.clf()
    hist_year_months(plt.gca(), hist_data['start_month'])
    plt.xlabel("Month")
    plt.ylabel("Number of Trips")
    plt.title("Number of Trips per Month")
    plt.tight_layout()
    plt.savefig("out/trips_per_month.png")

    plt.clf()
    plt.hist(hist_data['distances'], range=(0, 100), bins=20)
    plt.gca().xaxis.set_major_locator(MaxNLocator(nbins=20))
    plt.xlim(0, 100)
    plt.xlabel("Distance in km")
    plt.ylabel("Number of Trips")
    plt.title("Number of Trips per Distance")
    plt.tight_layout()
    plt.savefig("out/trips_per_distance.png")

    plt.clf()
    plt.hist(hist_data['trip_temp'], range=(-25, 30), bins=25)
    plt.xlim(-25, 35)
    plt.xlabel("Box Temperature °C")
    plt.ylabel("Number of Trips")
    plt.title("Number of Trips per Temperature")
    plt.tight_layout()
    plt.savefig("out/trips_per_temperature.png")

    plt.clf()
    hist_duration_minutes(plt.gca(), hist_data['durations'], interval=10, count=18, fmt=lambda x, pos: str(int(x)))
    plt.xlabel("Duration in Minutes")
    plt.ylabel("Number of Trips")
    plt.title("Number of Trips per Duration")
    plt.tight_layout()
    plt.savefig("out/trips_per_duration.png")

    plt.clf()
    bins = np.linspace(
        min(hist_data['initial_soc'] + hist_data['final_soc']),
        max(hist_data['initial_soc'] + hist_data['final_soc']), 30)
    hist_initial = plt.hist(hist_data['initial_soc'], bins=bins, label='initial')
    hist_final = plt.hist(hist_data['final_soc'], bins=bins, label='final')
    order_hists([hist_initial, hist_final])
    plt.xlim(0, 100)
    plt.xlabel("SoC")
    plt.ylabel("Number of Trips")
    plt.title("Number of Trips with certain Initial and Final State of Charge")
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.savefig("out/trips_per_soc.png")
