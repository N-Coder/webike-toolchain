import Trips
import Weather

__author__ = 'Niko Fink'

if __name__ == "__main__":
    from DB import qcursor, cursor, connection

    trip_hist_data = Trips.extract_hist(qcursor)
    Trips.plot_trips(trip_hist_data)

    weather_hist_data = Weather.extract_hist(Weather.read_data_db(cursor))
    Weather.plot_weather(
        {
            'weather': weather_hist_data,
            'trip': trip_hist_data['trip_weather']
        },
        out_file='out/trips_per_weather_{}.png'
    )

    connection.close()
