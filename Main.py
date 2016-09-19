import os
import warnings

import pymysql

import SoC
import Trips
import WeatherGC
import WeatherWU

__author__ = 'Niko Fink'

if __name__ == "__main__":
    warnings.filterwarnings('error', category=pymysql.Warning)
    connection = pymysql.connect(
        host="localhost",
        port=3306,
        user=os.environ['MYSQL_USER'],
        passwd=os.environ['MYSQL_PASSWORD'],
        db="webike"
    )
    try:
        SoC.preprocess_estimates(connection)
        connection.commit()

        Trips.preprocess_trips(connection)
        trip_hist_data = Trips.extract_hist(connection)
        Trips.plot_trips(trip_hist_data)
        connection.commit()

        gc_files = WeatherGC.download_data()
        gc_csv_data = WeatherGC.parse_data(gc_files)
        WeatherGC.write_data_csv(gc_csv_data)
        gc_db_data = WeatherGC.write_data_db(connection, gc_csv_data)
        connection.commit()

        wu_missing_data = WeatherWU.select_missing_dates(connection)
        WeatherWU.download_wunderg(connection, wu_missing_data)
        connection.commit()

        gc_hist_data = WeatherGC.extract_hist(gc_db_data)
        WeatherGC.plot_weather(
            {
                # 'metar': # TODO plot WU
                'weather': gc_hist_data,
                'trip': trip_hist_data['trip_weather']
            },
            out_file='out/trips_per_weather_{}.png'
        )

        connection.commit()
    except:
        connection.rollback()
        raise
    finally:
        connection.close()
