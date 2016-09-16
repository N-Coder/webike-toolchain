import os

import matplotlib.pyplot as plt
import pymysql

from Constants import IMEIS
from Weather import append_hist, plot_weather, SQL_MAPPING, extract_hist, read_data_db

__author__ = 'Niko Fink'

connection = pymysql.connect(
    host="tornado.cs.uwaterloo.ca",
    port=3306,
    user=os.environ['MYSQL_USER'],
    passwd=os.environ['MYSQL_PASSWORD'],
    db="webike"
)
cursor = connection.cursor(pymysql.cursors.DictCursor)

start_times = []
distances = []
initial_soc = []
final_soc = []
trip_weather = {}
for k, v in SQL_MAPPING.items():
    trip_weather[v] = []

for imei in IMEIS:
    print('Processing IMEI ' + imei)

    cursor.execute("SELECT * FROM trip%s" % imei)
    trips = cursor.fetchall()
    for trip in trips:
        print('Processing Trip ' + imei + '#' + str(trip['id']))

        # unfortunately, we can't use prepared statements as the table names change
        # (table and column names have to be static in SQL)
        cursor.execute("SELECT * FROM imei{} WHERE Stamp > '{}' ORDER BY Stamp ASC LIMIT 1"
                       .format(imei, trip['start_time']))
        first_sample = cursor.fetchone()
        cursor.execute("SELECT * FROM imei{} WHERE Stamp < '{}' ORDER BY Stamp DESC LIMIT 1"
                       .format(imei, trip['end_time']))
        last_sample = cursor.fetchone()
        cursor.execute("SELECT *, ABS(TIMESTAMPDIFF(SECOND, datetime, '{}')) AS diff "
                       "FROM webike_sfink.weather ORDER BY diff LIMIT 1"
                       .format(trip['start_time']))
        weather_sample = cursor.fetchone()

        start_times.append(first_sample['Stamp'].replace(year=2000, month=1, day=1))
        if trip['distance'] is not None:
            distances.append(float(trip['distance']))
        if first_sample['ChargingCurr'] is not None:
            initial_soc.append(float(first_sample['ChargingCurr']))  # TODO fix range
        if last_sample['ChargingCurr'] is not None:
            final_soc.append(float(last_sample['ChargingCurr']))  # TODO fix range
        for key, val in weather_sample.items():
            append_hist(trip_weather, key, val)

print('Plotting graphs')
figcnt = 0
plt.figure(figcnt)
plt.hist(start_times, bins=24)
plt.xlabel('Time of Day')
plt.ylabel('Number of Trips')
plt.title('Number of Trips per Hour of Day')
plt.savefig('out/trips_per_hour.png')

figcnt += 1
plt.figure(figcnt)
plt.hist(distances, bins=25)
plt.xlabel('Distance')
plt.ylabel('Number of Trips')
plt.title('Number of Trips per Distance')
plt.savefig('out/trips_per_distance.png')

figcnt += 1
plt.figure(figcnt)
plt.hist(initial_soc, bins=25, alpha=0.5)
plt.hist(final_soc, bins=25, alpha=0.5)
plt.xlabel('SoC')
plt.ylabel('Number of Trips')
plt.title('Number of Trips with certain Initial and Final State of Charge')
plt.savefig('out/trips_per_soc.png')

average_weather = extract_hist(read_data_db(cursor))
plot_weather(average_weather, fig_offset=figcnt, facecolor='green', alpha=0.5, normed=True, label_prefix='average-')
figcnt = plot_weather(trip_weather, out_file='out/trips_per_weather_{}.png', fig_offset=figcnt, facecolor='blue',
                      alpha=0.5, normed=True, label_prefix='trip-')

# plt.show()
connection.close()
