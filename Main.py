import DB
import Trips
import WeatherGC

__author__ = 'Niko Fink'
assert __name__ == "__main__"

with DB.connect() as connection:
    trip_hist_data = Trips.extract_hist(connection)
    Trips.plot_trips(trip_hist_data)

    gc_db_data = WeatherGC.read_data_db(connection)
    gc_hist_data = WeatherGC.extract_hist(gc_db_data)
    WeatherGC.plot_weather(
        {
            # 'metar': # TODO plot WU
            'weather': gc_hist_data,
            'trip': trip_hist_data['trip_weather']
        },
        out_file='out/trips_per_weather_{}.png'
    )
