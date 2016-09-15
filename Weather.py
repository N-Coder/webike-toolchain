import csv
import os
import sys
from datetime import datetime

import wget
from dateutil.relativedelta import relativedelta

from Constants import plot_weather

CSV_HEADER = ['Date/Time', 'Year', 'Month', 'Day', 'Time', 'Data Quality', 'Temp (°C)', 'Temp Flag',
              'Dew Point Temp (°C)', 'Dew Point Temp Flag', 'Rel Hum (%)', 'Rel Hum Flag',
              'Wind Dir (10s deg)', 'Wind Dir Flag', 'Wind Spd (km/h)', 'Wind Spd Flag', 'Visibility (km)',
              'Visibility Flag', 'Stn Press (kPa)', 'Stn Press Flag', 'Hmdx', 'Hmdx Flag', 'Wind Chill',
              'Wind Chill Flag', 'Weather']
SQL_MAPPING = {'Date/Time': 'datetime', 'Data Quality': 'quality', 'Temp (°C)': 'temp', 'Temp Flag': 'temp_flag',
               'Dew Point Temp (°C)': 'dew_point', 'Dew Point Temp Flag': 'dew_point_flag', 'Rel Hum (%)': 'rel_hum',
               'Rel Hum Flag': 'rel_hum_flag', 'Wind Dir (10s deg)': 'wind_dir', 'Wind Dir Flag': 'wind_dir_flag',
               'Wind Spd (km/h)': 'wind_speed', 'Wind Spd Flag': 'wind_speed_flag', 'Visibility (km)': 'visibility',
               'Visibility Flag': 'visibility_flag', 'Stn Press (kPa)': 'stn_press', 'Stn Press Flag': 'stn_press_flag',
               'Hmdx': 'hmdx', 'Hmdx Flag': 'hmdx_flag', 'Wind Chill': 'wind_chill',
               'Wind Chill Flag': 'wind_chill_flag', 'Weather': 'weather'}
SQL_COLUMNS = list(map(lambda x: SQL_MAPPING[x], filter(lambda x: x in SQL_MAPPING, CSV_HEADER)))
SQL_INSERT = "REPLACE INTO webike_sfink.weather ({}) VALUES ({});" \
    .format(", ".join(SQL_COLUMNS), ", ".join(["%s"] * len(SQL_COLUMNS)))

downloadDir = 'tmp/weather.gc.ca/'
files = []
data = []

print('Downloading weather.gc.ca data using cache directory ' + downloadDir)
if not os.path.exists(downloadDir):
    print('Created cache directory')
    os.makedirs(downloadDir)
for year in range(2014, datetime.now().year + 1):
    for month in range(1, 12 + 1):
        file = '{}{}-{}.csv'.format(downloadDir, year, month)
        files.append(file)
        endofmonth = datetime(year=year, month=month, day=1) + relativedelta(months=+1)

        if os.path.exists(file):
            mtime = datetime.fromtimestamp(os.path.getmtime(file))
            if mtime > endofmonth:
                print('Using cached version of {} last modified on {}'
                      .format(file, mtime))
                continue
            else:
                print('Removing outdated version of ' + file)
                os.remove(file)

        print('Downloading ' + file)
        wget.download(
            "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
            "format=csv&stationID=48569&Year={year}&Month={month}&Day=1&timeframe=1&submit= Download+Data"
                .format(year=year, month=month),
            out=file)

print('Download complete, got {} files'.format(len(files)))

for file in files:
    print('Parsing ' + file)

    with open(file, newline='', encoding='cp1250') as f:
        reader = csv.reader(f)
        try:
            skip = True
            for row in reader:
                if not skip:
                    data.append(row)
                if row == CSV_HEADER:
                    skip = False
            if skip:
                sys.exit('Invalid csv file {} missing header'.format(file))
        except csv.Error as e:
            sys.exit('Invalid csv file {}, line {}: {}'.format(file, reader.line_num, e))
print('{} entries parsed'.format(len(data)))
data.sort(key=lambda row: row[0])

if not os.path.exists('out'):
    os.makedirs('out')
with open('out/weather.csv', 'w', newline='') as f:
    print('Writing data to {}'.format(f.name))
    writer = csv.writer(f)
    writer.writerow(CSV_HEADER)
    for row in data:
        writer.writerow(row)
    print('{} lines written'.format(len(data)))

# print('Writing data to DB')
# connection = pymysql.connect(
#     host="tornado.cs.uwaterloo.ca",
#     port=3306,
#     user=os.environ['MYSQL_USER'],
#     passwd=os.environ['MYSQL_PASSWORD'],
#     db="webike"
# )
# cursor = connection.cursor(pymysql.cursors.DictCursor)
# warnings.filterwarnings('error', category=pymysql.Warning)
# # TODO check if DB data is up to date
# insert_cnt = 0
# replace_cnt = 0
# skip_cnt = 0
# try:
#     for row in data:
#         del row[1:5]  # delete redundant time information ('Year', 'Month', 'Day', 'Time')
#         if len(row) <= 1:  # skip entries which have no data
#             skip_cnt += 1
#             continue
#         assert len(row) == len(SQL_COLUMNS)
#
#
#         def clean(v):
#             if v == "‡":
#                 return "P"
#             elif v == "":
#                 return None
#             else:
#                 return v
#
#
#         row = list(map(clean, row))
#         res = 0
#         try:
#             res = cursor.execute(SQL_INSERT, row)
#         except:
#             print("Exception for row {}".format(row))
#             raise
#         if res == 1:
#             insert_cnt += 1
#         elif res == 2:
#             replace_cnt += 1
#         else:
#             raise AssertionError("Illegal result {} for row {}".format(res, row))
#         if (insert_cnt + replace_cnt + skip_cnt) % 1000 == 0:
#             print('{} rows inserted, {} rows replaced, {} rows skipped'.format(insert_cnt, replace_cnt, skip_cnt))
#
#     connection.commit()
#     print('{} rows inserted, {} rows replaced, {} rows skipped'.format(insert_cnt, replace_cnt, skip_cnt))
#     print('{}/{} rows written to DB'.format(insert_cnt + replace_cnt + skip_cnt, len(data)))
#     assert insert_cnt + replace_cnt + skip_cnt == len(data)
# except:
#     connection.rollback()
#     raise

print('Generating Graphs')
hist_data = {}
for col in SQL_COLUMNS:
    hist_data[col] = []
for row in data:
    if len(row) <= 1:  # skip entries which have no data
        continue
    del row[1:5]
    row = dict(zip(SQL_COLUMNS, row))
    for key, val in row.items():
        if val is not None and val != '':
            try:
                hist_data[key].append(float(val))
            except ValueError:
                hist_data[key].append(val)

plot_weather(hist_data, 'out/weather_{}.png')

print('Graphs finished')
