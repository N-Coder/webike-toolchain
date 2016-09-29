import logging
import threading
from datetime import timedelta, datetime

import gi
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import numpy as np
from dateutil.relativedelta import relativedelta
from gi.repository import Gtk, GLib, GObject
from matplotlib.backends.backend_gtk3 import NavigationToolbar2GTK3 as NavigationToolbar
from matplotlib.backends.backend_gtk3cairo import FigureCanvasGTK3Cairo as FigureCanvas
from matplotlib.figure import Figure

from util import DB
from util.DB import DictCursor
from util.Logging import BraceMessage as __

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")

gi.require_version('Gtk', '3.0')


def set_processing(processing):
    builder.get_object('prevButton').set_sensitive(not processing)
    builder.get_object('redrawButton').set_visible(not processing)
    builder.get_object('nextButton').set_sensitive(not processing)
    builder.get_object('redrawSpinner').set_visible(processing)


def draw_figure():
    year = int(builder.get_object('yearButton').get_text())
    month = int(builder.get_object('monthButton').get_text())
    imei = builder.get_object('imeiCombo').get_active_text()
    min = datetime(year=year, month=month, day=1)
    max = min + relativedelta(months=1) - timedelta(seconds=1)

    set_processing(True)

    logger.info(__("Plotting {} -- {}-{} from {} to {}", imei, year, month, min, max))
    thread = threading.Thread(target=get_data_async, args=(imei, min, max))
    thread.daemon = True
    thread.start()
    # get_data_async(imei, min, max)


def get_data_async(imei, min, max):
    cursor.execute(
        """SELECT Stamp, ChargingCurr, DischargeCurr, soc_smooth FROM imei{imei}
        JOIN webike_sfink.soc ON Stamp = time AND imei = '{imei}'
        WHERE Stamp >= '{min}' AND Stamp <= '{max}'
        ORDER BY Stamp ASC"""
            .format(imei=imei, min=min, max=max))
    charge_values = cursor.fetchall()

    smooth(charge_values, 'ChargingCurr')
    smooth(charge_values, 'DischargeCurr')

    cursor.execute("SELECT * FROM webike_sfink.charge_cycles WHERE imei='{}' ORDER BY start_time".format(imei))
    charge_cycles = cursor.fetchall()

    cursor.execute("SELECT * FROM trip{} ORDER BY start_time ASC".format(imei))
    trips = cursor.fetchall()

    GLib.idle_add(draw_figure_async, imei, min, max, charge_values, charge_cycles, trips)
    # draw_figure_async(imei, min, max, charge_values, charge_cycles, trips)


def discharge_curr_to_ampere(val):
    """Convert DischargeCurr from the DB from the raw sensor value to amperes"""
    return (val - 504) * 0.033 if val else 0


def smooth(samples, label, label_smooth=None, alpha=.95, default_value=None):
    """Smooth values using the formula
    `samples[n][label_smooth] = alpha * samples[n-1][label_smooth] + (1 - alpha) * samples[n][label]`
    If a value isn't available, the previous smoothed value is used.
    If none of these exist, default_value is used
    :param samples: a list of dicts
    :param label:
    :param label_smooth:
    :param alpha:
    :param default_value:
    :return:
    """
    if not label_smooth:
        label_smooth = label + '_smooth'

    last_sample = None
    for sample in samples:
        if not (sample and label in sample and sample[label]):
            sample[label_smooth] = default_value
        else:
            if not (last_sample and label_smooth in last_sample and
                        last_sample[label_smooth]):
                # 1nd sensible value in the list, use it as starting point for the smoothing
                sample[label_smooth] = sample[label]
            else:
                # current and previous value available, apply the smoothing function
                sample[label_smooth] = alpha * last_sample[label_smooth] \
                                       + (1 - alpha) * sample[label]
        last_sample = sample


def draw_figure_async(imei, min, max, charge_values, charge_cycles, trips):
    logger.info(__("Started rendering {} -- {}-{} from {} to {}", imei, min.year, min.month, min, max))

    ax.clear()
    ax.set_xlim(min, max)
    ax.plot(
        list([x['Stamp'] for x in charge_values]),
        list([x['soc_smooth'] or np.nan for x in charge_values]),
        'b-', label="State of Charge", alpha=0.9
    )
    ax.plot(
        list([x['Stamp'] for x in charge_values]),
        list([x['ChargingCurr_smooth'] / 200 if x['ChargingCurr'] else np.nan for x in charge_values]),
        'g-', label="Charging Current", alpha=0.9
    )
    ax.plot(
        list([x['Stamp'] for x in charge_values]),
        list([-discharge_curr_to_ampere(x['DischargeCurr_smooth']) if x['DischargeCurr'] else np.nan
              for x in charge_values]),
        'r-', label="Discharging Current", alpha=0.9
    )

    for trip in trips:
        ax.axvspan(trip['start_time'], trip['end_time'], color='y', alpha=0.5, lw=0)
    for cycle in charge_cycles:
        ax.axvspan(cycle['start_time'], cycle['end_time'], color=('m' if cycle['type'] == 'D' else 'c'),
                   alpha=0.5, lw=0)

    handles = list(ax.get_legend_handles_labels()[0])
    handles.append(mpatches.Patch(color='y', label='Trips'))
    handles.append(mpatches.Patch(color='c', label='Charging Cycles [ChargingCurr]'))
    handles.append(mpatches.Patch(color='m', label='Charging Cycles [DischargeCurr]'))
    ax.legend(handles=handles, loc='upper right')

    ax.set_title("{} -- {}-{}".format(imei, min.year, min.month))
    ax.set_xlim(min, max)
    ax.set_ylim(-3, 5)
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d'))
    fig.tight_layout()
    fig.canvas.draw()

    set_processing(False)
    logger.info(__("Finished plotting {} -- {}-{} from {} to {}", imei, min.year, min.month, min, max))


class Signals:
    def on_window_destroy(self, widget):
        Gtk.main_quit()

    def on_redraw(self, widget):
        draw_figure()

    def do_previous(self, widget):
        builder.get_object('monthButton').spin(Gtk.SpinType.STEP_BACKWARD, 1)
        draw_figure()

    def do_next(self, widget):
        builder.get_object('monthButton').spin(Gtk.SpinType.STEP_FORWARD, 1)
        draw_figure()


with DB.connect() as connection:
    with connection.cursor(DictCursor) as cursor:
        GObject.threads_init()

        builder = Gtk.Builder()
        builder.add_from_file('glade/timeline.glade')
        builder.connect_signals(Signals())

        window = builder.get_object('window')
        set_processing(True)

        fig = Figure()
        ax = fig.add_subplot(111)

        canvas = FigureCanvas(fig)
        builder.get_object('plotContainer').add(canvas)

        toolbar = NavigationToolbar(canvas, window)
        builder.get_object('toolbarContainer').add(toolbar)

        draw_figure()

        window.show_all()
        Gtk.main()
