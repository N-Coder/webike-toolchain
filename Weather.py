import csv
import os
import sys
import wget
from datetime import datetime

downloadDir = 'weather_download/'
if not os.path.exists(downloadDir):
    os.makedirs(downloadDir)
for year in range(2014, datetime.now().year + 1):
    for month in range(1, 12 + 1):
        print('loading {}-{}'.format(year, month))
        wget.download(
            "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
            "format=csv&stationID=48569&Year={year}&Month={month}&Day=1&timeframe=1&submit= Download+Data"
                .format(year=year, month=month),
            out=downloadDir)

print('files loaded')

HEADER = ['Date/Time', 'Year', 'Month', 'Day', 'Time', 'Data Quality', 'Temp (°C)', 'Temp Flag',
          'Dew Point Temp (°C)', 'Dew Point Temp Flag', 'Rel Hum (%)', 'Rel Hum Flag',
          'Wind Dir (10s deg)', 'Wind Dir Flag', 'Wind Spd (km/h)', 'Wind Spd Flag', 'Visibility (km)',
          'Visibility Flag', 'Stn Press (kPa)', 'Stn Press Flag', 'Hmdx', 'Hmdx Flag', 'Wind Chill',
          'Wind Chill Flag', 'Weather']

rows = []

for file in os.listdir(downloadDir):
    if not file.endswith(".csv"):
        print('skipping ' + file)
        continue

    with open(downloadDir + file, newline='', encoding='cp1250') as f:
        reader = csv.reader(f)
        try:
            skip = True
            for row in reader:
                if not skip:
                    rows.append(row)
                if row == HEADER:
                    skip = False
            if skip:
                sys.exit('invalid csv file {} missing header'.format(file))
            else:
                print('parsed ' + file)
        except csv.Error as e:
            sys.exit('invalid csv file {}, line {}: {}'.format(file, reader.line_num, e))

print('%s entries read' % len(rows))
rows.sort(key=lambda row: row[0])

with open('weather.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(HEADER)
    for row in rows:
        writer.writerow(row)

print('%s lines written' % len(rows))
