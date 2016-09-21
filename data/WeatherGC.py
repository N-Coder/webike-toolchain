import copy
import csv
import logging
import os
from datetime import datetime
from decimal import Decimal

import wget
from dateutil.relativedelta import relativedelta

from util.DB import DictCursor
from util.Logging import BraceMessage as __

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)

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
DOWNLOAD_DIR = "tmp/weather.gc.ca/"
HIST_DATA = dict([(v, []) for k, v in SQL_MAPPING.items()])


def download_data():
    files = []
    logger.info(__("Downloading weather.gc.ca data using cache directory {}", DOWNLOAD_DIR))
    if not os.path.exists(DOWNLOAD_DIR):
        logger.info("Created cache directory")
        os.makedirs(DOWNLOAD_DIR)
    for year in range(2014, datetime.now().year + 1):
        for month in range(1, 12 + 1):
            file = "{}{}-{}.csv".format(DOWNLOAD_DIR, year, month)
            end_of_month = datetime(year=year, month=month, day=1) + relativedelta(months=1)

            if datetime(year=year, month=month, day=1) > datetime.now():  # don't download future months
                continue

            files.append(file)
            if os.path.exists(file):
                mtime = datetime.fromtimestamp(os.path.getmtime(file))
                if mtime > end_of_month:
                    logger.debug(__("Using cached version of {} last modified on {}",
                                    file, mtime))
                    continue
                else:
                    logger.info(__("Removing outdated version of {}", file))
                    os.remove(file)

            logger.info(__("Downloading {}", file))
            wget.download(
                "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
                "format=csv&stationID=48569&Year={year}&Month={month}&Day=1&timeframe=1&submit= Download+Data"
                    .format(year=year, month=month),
                out=file, bar=None)
    logger.info(__("Download complete, got {} files", len(files)))
    return files


def parse_data(files):
    data = []
    latest = datetime.min
    for file in files:
        logger.debug(__("Parsing {}", file))

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
                    raise ValueError("Invalid csv file {} missing header".format(file))
            except csv.Error as e:
                raise ValueError("Invalid csv file {}, line {}".format(file, reader.line_num)) from e
    logger.info(__("{} entries parsed, latest one from {}", len(data), latest))
    data.sort(key=lambda row: row[0])
    return data


def write_data_csv(data):
    with open(DOWNLOAD_DIR + "weather.csv", 'w', newline='') as f:
        logger.info(__("Writing w data to {}", f.name))
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for row in data:
            writer.writerow(row)
        logger.info(__("{} lines written", len(data)))


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


def write_data_db(connection, csv_data):
    logger.info("Writing data to DB")

    with connection.cursor(DictCursor) as cursor:
        cursor.execute("SELECT datetime FROM webike_sfink.weather ORDER BY datetime DESC LIMIT 1")
        db_latest = cursor.fetchone()['datetime']
        cursor.execute("SELECT COUNT(*) AS count FROM webike_sfink.weather")
        db_count = cursor.fetchone()['count']
        logger.info(__("DB already contains {} rows, with the latest being dated {}",
                       db_count, db_latest))

        insert_cnt = 0
        skip_cnt = 0
        existing_cnt = 0
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
                elif res == 2:
                    existing_cnt += 1
                else:
                    raise AssertionError("Illegal result {} for row {}".format(res, row))
            except:
                logger.info(__("Exception for row {}", row))
                raise

            if (insert_cnt + skip_cnt) % 1000 == 0:
                logger.info(__("{} rows inserted, {} empty rows skipped, {} rows older than latest change skipped",
                               insert_cnt, skip_cnt, existing_cnt))

        logger.info(__("{} rows inserted, {} empty rows skipped, {} rows older than latest change skipped",
                       insert_cnt, skip_cnt, existing_cnt))
        logger.info(__("{} of {} rows from csv parsed",
                       insert_cnt + existing_cnt + skip_cnt, len(csv_data)))
        logger.info(__("DB now contains {} + {} = {} of {} relevant rows",
                       db_count, insert_cnt, db_count + insert_cnt, len(db_data)))
        assert insert_cnt + existing_cnt + skip_cnt == len(csv_data)
        assert insert_cnt + existing_cnt == len(db_data)
        assert existing_cnt == db_count

        return db_data


def read_data_db(connection):
    logger.info("Reading weather.gc data from DB")

    with connection.cursor(DictCursor) as cursor:
        cursor.execute("SELECT * FROM webike_sfink.weather ORDER BY datetime DESC")
        data = cursor.fetchall()
        logger.info(__("{} rows read from DB", len(data)))
        return data


def extract_hist(data):
    hist_data = copy.deepcopy(HIST_DATA)
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
