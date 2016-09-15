import csv
import os
import sys
import warnings
from collections import Counter
from datetime import datetime
from decimal import Decimal

import matplotlib.pyplot as plt
import numpy as np
import pymysql
import wget
from dateutil.relativedelta import relativedelta

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


def download_data():
    download_dir = 'tmp/weather.gc.ca/'
    files = []
    print('Downloading weather.gc.ca data using cache directory ' + download_dir)
    if not os.path.exists(download_dir):
        print('Created cache directory')
        os.makedirs(download_dir)
    for year in range(2014, datetime.now().year + 1):
        for month in range(1, 12 + 1):
            file = '{}{}-{}.csv'.format(download_dir, year, month)
            end_of_month = datetime(year=year, month=month, day=1) + relativedelta(months=+1)

            if datetime(year=year, month=month, day=1) > datetime.now():  # don't download future months
                continue

            files.append(file)
            if os.path.exists(file):
                mtime = datetime.fromtimestamp(os.path.getmtime(file))
                if mtime > end_of_month:
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
                out=file, bar=None)
    print('Download complete, got {} files'.format(len(files)))
    return files


def parse_data(files):
    data = []
    latest = datetime.min
    for file in files:
        print('Parsing ' + file)

        with open(file, newline='', encoding='cp1250') as f:
            reader = csv.reader(f)
            try:
                skip = True
                for row in reader:
                    if not skip:
                        data.append(row)
                        latest = max(latest, datetime.strptime(row[0], '%Y-%m-%d %H:%M'))
                    if row == CSV_HEADER:
                        skip = False
                if skip:
                    sys.exit('Invalid csv file {} missing header'.format(file))
            except csv.Error as e:
                sys.exit('Invalid csv file {}, line {}: {}'.format(file, reader.line_num, e))
    print('{} entries parsed, latest one from {}'.format(len(data), latest))
    data.sort(key=lambda row: row[0])
    return data


def write_data_csv(data):
    if not os.path.exists('out'):
        os.makedirs('out')
    with open('out/weather.csv', 'w', newline='') as f:
        print('Writing data to {}'.format(f.name))
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for row in data:
            writer.writerow(row)
        print('{} lines written'.format(len(data)))


def write_data_db(cursor, data):
    print('Writing data to DB')

    cursor.execute("SELECT * FROM webike_sfink.weather ORDER BY datetime DESC LIMIT 1")
    db_latest = cursor.fetchone()['datetime']

    cursor.execute("SELECT COUNT(*) AS count FROM webike_sfink.weather")
    db_count = cursor.fetchone()['count']
    print('DB already contains {} rows, with the latest being dated {}'
          .format(db_count, db_latest))

    insert_cnt = 0
    skip_cnt = 0
    existing_cnt = 0
    try:
        for row in data:
            del row[1:5]  # delete redundant time information ('Year', 'Month', 'Day', 'Time')
            if len(row) <= 1:  # skip entries which have no data
                skip_cnt += 1
                continue
            assert len(row) == len(SQL_COLUMNS)

            if datetime.strptime(row[0], '%Y-%m-%d %H:%M') <= db_latest:
                existing_cnt += 1
                continue

            def clean(v):
                if v == "‡":
                    return "P"
                elif v == "":
                    return None
                else:
                    return v

            row = list(map(clean, row))
            try:
                res = cursor.execute(SQL_INSERT, row)
            except:
                print("Exception for row {}".format(row))
                raise
            if res == 1:
                insert_cnt += 1
            else:
                raise AssertionError("Illegal result {} for row {}".format(res, row))
            if (insert_cnt + skip_cnt) % 1000 == 0:
                print('{} rows inserted, {} empty rows skipped, {} rows already existed'
                      .format(insert_cnt, skip_cnt, existing_cnt))

        connection.commit()
        print('{} rows inserted, {} empty rows skipped, {} rows older than latest change skipped'
              .format(insert_cnt, skip_cnt, existing_cnt))
        print('DB now contains {} + {} = {} of {} relevant rows'
              .format(db_count, insert_cnt, db_count + insert_cnt, len(data) - skip_cnt))
        assert insert_cnt + existing_cnt + skip_cnt == len(data)
        assert db_count == existing_cnt
    except:
        connection.rollback()
        raise


def read_data_db(cursor):
    print('Reading data from DB')

    cursor.execute("SELECT * FROM webike_sfink.weather ORDER BY datetime DESC")
    data = cursor.fetchall()
    print('{} rows read from DB'.format(len(data)))
    return data


def extract_hist(data):
    hist_data = {}
    for col in SQL_COLUMNS:
        hist_data[col] = []
    for row in data:
        for key, val in row.items():
            append_hist(hist_data, key, val)
    return hist_data


def append_hist(hist_data, key, val):
    if val is not None and key in hist_data:
        if isinstance(val, Decimal):
            hist_data[key].append(float(val))
        else:
            hist_data[key].append(val)


def plot_weather(hist_data, filename):
    print('Plotting graphs')
    for key, value in hist_data.items():
        plt.clf()
        if key == 'weather':
            counter = Counter(value)
            frequencies = counter.values()
            names = counter.keys()
            x_coordinates = np.arange(len(counter))
            plt.bar(x_coordinates, frequencies, align='center')
            plt.xticks(x_coordinates, names)
        elif key.endswith('_flag') or key == 'datetime' or key == 'quality':
            continue
        else:
            plt.hist(value, bins=25)
        plt.title('Weather - ' + key)
        plt.savefig(filename.format(key))

    print('Graphs finished')


if __name__ == "__main__":
    connection = pymysql.connect(
        host="tornado.cs.uwaterloo.ca",
        port=3306,
        user=os.environ['MYSQL_USER'],
        passwd=os.environ['MYSQL_PASSWORD'],
        db="webike"
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    warnings.filterwarnings('error', category=pymysql.Warning)

    files = download_data()
    csv_data = parse_data(files)
    write_data_csv(csv_data)
    write_data_db(cursor, csv_data)

    db_data = read_data_db(cursor)
    hist_data = extract_hist(db_data)
    plot_weather(hist_data, 'out/weather_{}.png')
