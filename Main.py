import os

import matplotlib.pyplot as plt
import pymysql

from Constants import IMEIS

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

for imei in IMEIS:
    print('Processing IMEI ' + imei)

    cursor.execute("SELECT * FROM trip%s" % imei)
    trips = cursor.fetchall()
    for trip in trips:
        print('Processing Trip ' + imei + '#' + str(trip['id']))

        cursor.execute(
            "SELECT * FROM imei%s WHERE Stamp > '%s' ORDER BY Stamp ASC LIMIT 1" % (imei, trip['start_time']))
        first_sample = cursor.fetchone()
        cursor.execute("SELECT * FROM imei%s WHERE Stamp < '%s' ORDER BY Stamp DESC LIMIT 1" % (imei, trip['end_time']))
        last_sample = cursor.fetchone()

        start_times.append(first_sample['Stamp'].replace(year=2000, month=1, day=1))
        if trip['distance'] is not None:
            distances.append(float(trip['distance']))
        # TODO fix range
        if first_sample['ChargingCurr'] is not None:
            initial_soc.append(float(first_sample['ChargingCurr']))
        if last_sample['ChargingCurr'] is not None:
            final_soc.append(float(last_sample['ChargingCurr']))

connection.close()

plt.figure(1)
plt.hist(start_times, bins=24)
plt.xlabel('Time of Day')
plt.ylabel('Number of Trips')
plt.title('Number of Trips per Hour of Day')
plt.savefig('out/trips_per_hour.png')

plt.figure(2)
plt.hist(distances, bins=25)
plt.xlabel('Distance')
plt.ylabel('Number of Trips')
plt.title('Number of Trips per Distance')
plt.savefig('out/trips_per_distance.png')

plt.figure(3)
plt.hist(initial_soc, bins=25, alpha=0.5)
plt.hist(final_soc, bins=25, alpha=0.5)
plt.xlabel('SoC')
plt.ylabel('Number of Trips')
plt.title('Number of Trips with certain Initial and Final State of Charge')
plt.savefig('out/trips_per_soc.png')

plt.show()
