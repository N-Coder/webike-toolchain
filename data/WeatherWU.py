import copy
import csv
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone, time

import requests
from metar import Metar

from util.DB import DictCursor
from util.Logging import BraceMessage as __

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)

HIST_DATA = {'temp': [], 'dewpt': [], 'wind_speed': [], 'vis': [], 'press': [], 'weather_desc': [], 'weather_prec': [],
             'weather_obsc': [], 'weather_othr': []}

DOWNLOAD_DIR = "tmp/wunderground/"
URL = "https://www.wunderground.com/history/airport/CYKF/{year}/{month}/{day}/DailyHistory.html?format=1"


def insert_navlost(connection):
    """Function for the one-time import of METAR data from navlost.eu"""
    logger.info("Loading navlost data")
    with connection.cursor(DictCursor) as cursor:
        with open("tmp/f0b74520-f7df-45e4-a596-f4392296296a.csv", 'rt') as f:
            reader = csv.reader(f, delimiter='\t')
            count = 0
            for row in reader:
                stamp = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                count += cursor.execute(
                    "REPLACE INTO webike_sfink.weather_metar (stamp, metar, source) VALUES (%s, %s, 'navlost')",
                    [stamp, "METAR " + row[3]])
                if count % 1000 == 0:
                    logger.info(__("{} rows inserted", count))
            logger.info(__("{} rows inserted", count))


def select_missing_dates(connection):
    logger.info("Selecting dates with missing data (this could take a few secs)...")
    with connection.cursor(DictCursor) as cursor:
        cursor.execute("""SELECT
              selected_date,
              COUNT(stamp) AS count,
              MIN(stamp) AS min,
              MAX(stamp) AS max
            FROM webike_sfink.datest
              LEFT OUTER JOIN webike_sfink.weather_metar ON selected_date = DATE(stamp)
            WHERE selected_date >=
                  (SELECT MIN(stamp)
                   FROM webike_sfink.weather_metar) AND
                  selected_date <=
                  DATE(NOW())
            GROUP BY selected_date
            HAVING count < 24 OR min > ADDTIME(selected_date, '00:00:00') OR max < ADDTIME(selected_date, '23:00:00')""")
        dates = cursor.fetchall()
        logger.info(__("{} dates having too few data", len(dates)))
        return dates


def download_wunderg(connection, dates):
    logger.info(__("Downloading weather underground data using cache directory {}", DOWNLOAD_DIR))
    if not os.path.exists(DOWNLOAD_DIR):
        logger.info("Created cache directory")
        os.makedirs(DOWNLOAD_DIR)

    with connection.cursor(DictCursor) as cursor:
        for entry in dates:
            # due to differences in the time zones, entries in the early morning in UTC are still on
            # the previous day in EST, so download the previous day, too
            if entry['min'] is None or entry['min'].time() > time(hour=0, minute=0):
                __download_wunderg_metar(cursor, entry['selected_date'] - timedelta(days=1))
            __download_wunderg_metar(cursor, entry['selected_date'])


def __download_wunderg_metar(cursor, date):
    logger.info(__("Downloading METAR data for {}", date))
    file = "{}{year}-{month}-{day}.csv".format(DOWNLOAD_DIR, year=date.year, month=date.month, day=date.day)

    if os.path.exists(file):
        mtime = datetime.fromtimestamp(os.path.getmtime(file))
        if mtime < (datetime.fromordinal(date.toordinal()) + timedelta(days=2)):
            logger.info(__("Removing outdated version of {}", file))
            os.remove(file)
        else:
            logger.info("File already exists, no new data available")
            return
    if not os.path.exists(file):
        res = requests.get(URL.format(year=date.year, month=date.month, day=date.day), stream=True,
                           cookies={"Prefs": "SHOWMETAR:1"})
        assert res.ok
        with open(file, 'wb') as f:
            res.raw.decode_content = True
            shutil.copyfileobj(res.raw, f)

    with open(file, 'rt') as f:
        text = f.read()
        if "No daily or hourly history data available" in text:
            logger.warn(__("No daily or hourly history data available from {}", file))
            return
        text = text.strip().replace("<br />", "").splitlines()
        reader = csv.DictReader(text, )
        count = 0
        for row in reader:
            time = datetime.strptime(row['DateUTC'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            metar = row['FullMetar']
            if metar.startswith('METAR') or metar.startswith('SPECI'):
                count += cursor.execute(
                    "INSERT INTO webike_sfink.weather_metar (stamp, metar, source) "
                    "VALUES (%s, %s, 'wunderg') ON DUPLICATE KEY UPDATE stamp=stamp",
                    [time, metar])
        logger.info(__("{} rows inserted", count))


def read_data_db(connection):
    logger.info("Reading weather underground data from DB")

    with connection.cursor(DictCursor) as cursor:
        cursor.execute("SELECT * FROM webike_sfink.weather_metar ORDER BY stamp DESC")
        data = cursor.fetchall()
        logger.info(__("{} rows read from DB", len(data)))
        return data


def extract_hist(metars):
    hist_data = copy.deepcopy(HIST_DATA)
    for metar in metars:
        if isinstance(metar, dict):
            metar = metar['metar']
        append_hist(hist_data, metar)
    return hist_data


def append_hist(hist_data, metar):
    if isinstance(metar, str):
        metar = Metar.Metar(metar)
    assert isinstance(metar, Metar.Metar)

    if metar.temp:
        hist_data['temp'].append(metar.temp.value("C"))
    if metar.dewpt:
        hist_data['dewpt'].append(metar.dewpt.value("C"))
    if metar.wind_speed:
        hist_data['wind_speed'].append(metar.wind_speed.value("KMH"))
    if metar.vis:
        hist_data['vis'].append(metar.vis.value("KM"))
    if metar.press:
        hist_data['press'].append(metar.press.value("MB"))

    if len(metar.weather) < 1:
        hist_data['weather_desc'].append("")
        hist_data['weather_prec'].append("")
        hist_data['weather_obsc'].append("")
        hist_data['weather_othr'].append("")
    else:
        for weather in metar.weather:
            (inten, desc, prec, obsc, othr) = weather
            hist_data['weather_desc'].append(desc)
            if len(prec) > 2:
                hist_data['weather_prec'].extend([prec[i:i + 2] for i in range(0, len(prec), 2)])
            else:
                hist_data['weather_prec'].append(prec)
            hist_data['weather_obsc'].append(obsc)
            hist_data['weather_othr'].append(othr)
