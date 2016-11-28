import logging

from iss4e.db.mysql import DictCursor
from iss4e.util import BraceMessage as __
from webike.util.constants import IMEIS

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


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
