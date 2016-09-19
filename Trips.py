import matplotlib.pyplot as plt
import numpy as np
from pymysql.cursors import DictCursor

import WeatherGC
from Constants import IMEIS
from DB import QualifiedDictCursor

__author__ = 'Niko Fink'


def preprocess_trips(connection):
    print('Preprocessing JOIN information for new trips')
    with connection.cursor(DictCursor) as cursor:
        for imei in IMEIS:
            cursor.execute("SELECT trip{imei}.* FROM trip{imei} LEFT JOIN webike_sfink.trips ON "
                           "trip{imei}.id = trips.trip AND trips.imei='{imei}' WHERE trips.trip IS NULL;"
                           .format(imei=imei))
            unprocessed_trips = cursor.fetchall()
            print('Processing {} new entries for IMEI {}'.format(len(unprocessed_trips), imei))
            for nr, trip in enumerate(unprocessed_trips):
                print('{} of {}: processing new trip {}#{}'
                      .format(nr + 1, len(unprocessed_trips), imei, trip['id']))

                # unfortunately, we can't use prepared statements as the table names change
                # (table and column names have to be static in SQL)
                cursor.execute("SELECT Stamp FROM imei{} WHERE Stamp > '{}' ORDER BY Stamp ASC LIMIT 1"
                               .format(imei, trip['start_time']))
                first_sample = cursor.fetchone()
                cursor.execute("SELECT Stamp FROM imei{} WHERE Stamp < '{}' ORDER BY Stamp DESC LIMIT 1"
                               .format(imei, trip['end_time']))
                last_sample = cursor.fetchone()
                cursor.execute("SELECT datetime, ABS(TIMESTAMPDIFF(SECOND, datetime, '{}')) AS diff "
                               "FROM webike_sfink.weather ORDER BY diff LIMIT 1"
                               .format(trip['start_time']))
                weather_sample = cursor.fetchone()

                res = cursor.execute(
                    "INSERT INTO webike_sfink.trips(imei, trip, start_time, end_time, distance, weather) VALUES "
                    "(%s,%s,%s,%s,%s,%s)",
                    (imei, trip['id'], first_sample['Stamp'], last_sample['Stamp'], trip['distance'],
                     weather_sample['datetime']))
                if res != 1:
                    raise AssertionError("Illegal result {} for row #{}: {}".format(res, nr, trip))


def extract_hist(connection):
    print('Generating histogram data')

    with connection.cursor(QualifiedDictCursor) as qcursor:
        hist_data = {
            'start_times': [],
            'distances': [],
            'initial_soc': [],
            'final_soc': [],
            'trip_weather': {}
        }
        for k, v in WeatherGC.SQL_MAPPING.items():
            hist_data['trip_weather'][v] = []

        for imei in IMEIS:
            print('Processing IMEI ' + imei)

            qcursor.execute(
                "SELECT * "
                "FROM webike_sfink.trips trip "
                "  JOIN imei{imei} first_sample ON first_sample.Stamp = trip.start_time "
                "  JOIN imei{imei} last_sample ON last_sample.Stamp = trip.end_time "
                "  JOIN webike_sfink.weather weather ON trip.weather = weather.datetime "
                "WHERE trip.imei = '{imei}'".format(imei=imei))
            trips = qcursor.fetchall()
            for trip in trips:
                hist_data['start_times'].append(trip['first_sample.Stamp'].replace(year=2000, month=1, day=1))

                if trip['trip.distance'] is not None:
                    hist_data['distances'].append(float(trip['trip.distance']))

                if trip['first_sample.ChargingCurr'] is not None:
                    hist_data['initial_soc'].append(float(trip['first_sample.ChargingCurr']))  # TODO fix range

                if trip['last_sample.ChargingCurr'] is not None:
                    hist_data['final_soc'].append(float(trip['last_sample.ChargingCurr']))  # TODO fix range

                weather_prefix = 'weather.'
                for key, val in trip.items():
                    if key.startswith(weather_prefix):
                        WeatherGC.append_hist(hist_data['trip_weather'], key[len(weather_prefix):], val)

        return hist_data


def plot_trips(hist_data):
    print('Plotting trip graphs')
    plt.clf()
    plt.hist(hist_data['start_times'], bins=24)
    plt.xlabel('Time of Day')
    plt.ylabel('Number of Trips')
    plt.title('Number of Trips per Hour of Day')
    plt.savefig('out/trips_per_hour.png')

    plt.clf()
    plt.hist(hist_data['distances'], bins=25)
    plt.xlabel('Distance')
    plt.ylabel('Number of Trips')
    plt.title('Number of Trips per Distance')
    plt.savefig('out/trips_per_distance.png')

    plt.clf()
    bins = np.linspace(
        min(hist_data['initial_soc'] + hist_data['final_soc']),
        max(hist_data['initial_soc'] + hist_data['final_soc']), 30)
    plt.hist(hist_data['initial_soc'], bins=bins, alpha=0.5, label='initial')
    plt.hist(hist_data['final_soc'], bins=bins, alpha=0.5, label='final')
    plt.xlabel('SoC')
    plt.ylabel('Number of Trips')
    plt.title('Number of Trips with certain Initial and Final State of Charge')
    plt.legend()
    plt.savefig('out/trips_per_soc.png')