import csv
import os
import sys
from collections import Counter
from datetime import datetime
from decimal import Decimal

import matplotlib.pyplot as plt
import numpy as np
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
        print('Writing w data to {}'.format(f.name))
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for row in data:
            writer.writerow(row)
        print('{} lines written'.format(len(data)))


def __clean_csv_value(k_v):
    k, v = k_v
    if v == "‡":
        return "P"
    elif v == "":
        return None
    elif k == 'datetime':
        return datetime.strptime(v, '%Y-%m-%d %H:%M')
    else:
        try:
            try:
                return int(v)
            except ValueError:
                return float(v)
        except ValueError:
            return v


def write_data_db(cursor, csv_data):
    print('Writing data to DB')

    cursor.execute("SELECT datetime FROM webike_sfink.weather ORDER BY datetime DESC LIMIT 1")
    db_latest = cursor.fetchone()['datetime']
    cursor.execute("SELECT COUNT(*) AS count FROM webike_sfink.weather")
    db_count = cursor.fetchone()['count']
    print('DB already contains {} rows, with the latest being dated {}'
          .format(db_count, db_latest))

    insert_cnt = 0
    skip_cnt = 0
    existing_cnt = 0
    try:
        db_data = []

        for row in csv_data:
            # Transform csv-like ordered list to db-like named dict
            row = zip(CSV_HEADER, row)
            row = [(SQL_MAPPING[k_v[0]], k_v[1]) for k_v in row if (k_v[0] in SQL_MAPPING)]
            row = [(k_v[0], __clean_csv_value(k_v)) for k_v in row]
            row = dict(row)
            # skip entries which have no data
            if len(row) <= 1:
                skip_cnt += 1
                continue
            assert len(row) == len(SQL_MAPPING)
            db_data.append(row)

            # skip entries which are probably already stored
            if row['datetime'] <= db_latest:
                existing_cnt += 1
                continue

            # insert the entry
            try:
                sql = "REPLACE INTO webike_sfink.weather ({}) VALUES ({});" \
                    .format(", ".join(row.keys()), ", ".join(["%s"] * len(row)))
                res = cursor.execute(sql, list(row.values()))
                if res == 1:
                    insert_cnt += 1
                else:
                    raise AssertionError("Illegal result {} for row {}".format(res, row))
            except:
                print("Exception for row {}".format(row))
                raise

            if (insert_cnt + skip_cnt) % 1000 == 0:
                print('{} rows inserted, {} empty rows skipped, {} rows older than latest change skipped'
                      .format(insert_cnt, skip_cnt, existing_cnt))

        print('{} rows inserted, {} empty rows skipped, {} rows older than latest change skipped'
              .format(insert_cnt, skip_cnt, existing_cnt))
        print('{} of {} rows from csv parsed'
              .format(insert_cnt + existing_cnt + skip_cnt, len(csv_data)))
        print('DB now contains {} + {} = {} of {} relevant rows'
              .format(db_count, insert_cnt, db_count + insert_cnt, len(db_data)))
        assert insert_cnt + existing_cnt + skip_cnt == len(csv_data)
        assert insert_cnt + existing_cnt == len(db_data)
        assert existing_cnt == db_count

        connection.commit()
        return db_data
    except:
        connection.rollback()
        raise


def read_data_db(cursor):
    print('Reading weather data from DB')

    cursor.execute("SELECT * FROM webike_sfink.weather ORDER BY datetime DESC")
    data = cursor.fetchall()
    print('{} rows read from DB'.format(len(data)))
    return data


def extract_hist(data):
    hist_data = {}
    for k, v in SQL_MAPPING.items():
        hist_data[v] = []
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


def plot_weather(hist_datasets, out_file=None, fig_offset=None):
    print('Plotting weather graphs')
    # get all keys in all datasets
    keys = {key for ds in hist_datasets.values() for key in ds.keys()}

    for key in keys:
        if key.endswith('_flag') or key == 'datetime' or key == 'quality':
            continue
        if fig_offset is not None:
            plt.figure(fig_offset)
            fig_offset += 1
        else:
            plt.clf()

        if key == 'weather':
            counters = []
            labels = set()
            for name, ds in hist_datasets.items():
                if key in ds:
                    counter = Counter([w for v in ds[key] for w in v.split(",")])
                    counter['NA'] = 0
                    counters.append((name, counter))
                    for label in counter.keys():
                        labels.add(label)

            x_coordinates = np.arange(len(labels))
            plt.xticks(x_coordinates, labels)
            for (name, counter) in counters:
                integral = sum(counter.values())
                freq = [counter[label] / integral * 100 for label in labels]
                plt.bar(x_coordinates, freq, align='center', label=name + '-' + key, alpha=0.5)
        else:
            value_lists = [(name, ds[key]) for name, ds in hist_datasets.items() if key in ds]
            min_val = min([min(l, default=-sys.maxsize) for n, l in value_lists])
            max_val = max([max(l, default=sys.maxsize) for n, l in value_lists])
            bins = np.linspace(min_val, max_val, 25)

            for name, vl in value_lists:
                plt.hist(vl, bins=bins, label=name + ' - ' + key, alpha=0.5, normed=True)

        plt.title('Weather - ' + key)
        plt.legend()
        if out_file is not None:
            plt.savefig(out_file.format(key))

    print('Graphs finished')
    return fig_offset


if __name__ == "__main__":
    from DB import cursor, connection

    files = download_data()
    csv_data = parse_data(files)
    write_data_csv(csv_data)
    db_data = write_data_db(cursor, csv_data)

    # db_data = read_data_db(cursor)
    hist_data = extract_hist(db_data)
    plot_weather({'weather': hist_data}, 'out/weather_{}.png')

    connection.close()
