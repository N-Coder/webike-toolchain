import gi

gi.require_version('Gtk', '3.0')

import logging
import threading
from datetime import timedelta, datetime

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import numpy as np
from dateutil.relativedelta import relativedelta
from gi.repository import Gtk, GLib, GObject
from matplotlib.backends.backend_gtk3cairo import FigureCanvasGTK3Cairo as FigureCanvas
from matplotlib.figure import Figure

from ui.Toolbar import PlotToolbar
from util import DB
from util.DB import DictCursor
from util.Logging import BraceMessage as __
from util.Utils import discharge_curr_to_ampere, smooth

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(threadName)-10.10s %(levelname)-3.3s"
                                                " %(name)-12.12s - %(message)s")


def draw_figure():
    logger.debug("enter draw_figure")
    year = int(builder.get_object('yearButton').get_text())
    month = int(builder.get_object('monthButton').get_text())
    imei = builder.get_object('imeiCombo').get_active_text()
    begin = datetime(year=year, month=month, day=1)
    end = begin + relativedelta(months=1) - timedelta(seconds=1)

    set_processing(True)

    logger.info(__("Plotting {} -- {}-{} from {} to {}", imei, year, month, begin, end))
    thread = threading.Thread(target=get_data_async, args=(imei, begin, end))
    thread.daemon = True
    thread.start()
    # get_data_async(imei, begin, end)


def get_data_async(imei, begin, end):
    logger.debug("enter get_data_async")
    cursor.execute(
        """SELECT Stamp, ChargingCurr, DischargeCurr, soc_smooth FROM imei{imei}
        JOIN webike_sfink.soc ON Stamp = time AND imei = '{imei}'
        WHERE Stamp >= '{min}' AND Stamp <= '{max}'
        ORDER BY Stamp ASC"""
            .format(imei=imei, min=begin, max=end))
    charge_values = cursor.fetchall()

    smooth(charge_values, 'ChargingCurr')
    smooth(charge_values, 'DischargeCurr')

    cursor.execute(
        "SELECT * FROM webike_sfink.charge_cycles "
        "WHERE imei='{imei}' AND end_time >= '{min}' AND start_time <= '{max}' "
        "ORDER BY start_time".format(imei=imei, min=begin, max=end))
    charge_cycles = cursor.fetchall()

    cursor.execute(
        "SELECT * FROM trip{imei} "
        "WHERE end_time >= '{min}' AND start_time <= '{max}' "
        "ORDER BY start_time ASC"
            .format(imei=imei, min=begin, max=end))
    trips = cursor.fetchall()

    logger.debug("leave get_data_async")
    # GLib.idle_add(draw_figure_async, imei, min, max, charge_values, charge_cycles, trips)
    draw_figure_async(imei, begin, end, charge_values, charge_cycles, trips)


def draw_figure_async(imei, begin, end, charge_values, charge_cycles, trips):
    logger.debug("enter draw_figure_async")

    legend_visible = not fig.add_subplot(111).legend_ or fig.add_subplot(111).legend_.get_visible()
    fig.clear()
    ax = fig.add_subplot(111)

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
    legend = ax.legend(handles=handles, loc='upper right')
    legend.set_visible(legend_visible)

    ax.set_title("{} -- {}-{}".format(imei, begin.year, begin.month))
    ax.set_xlim(begin, end)
    ax.set_ylim(-3, 5)
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d'))
    fig.tight_layout()

    logger.debug("leave draw_figure_async")
    GLib.idle_add(display_figure, imei, end, begin)
    # display_figure(imei, max, min)


def display_figure(imei, begin, end):
    logger.info(__("Finished plotting {} -- {}-{} from {} to {}", imei, end.year, end.month, end, begin))
    fig.canvas.draw()
    set_processing(False)
    logger.debug("leave display_figure")


def set_processing(processing):
    builder.get_object('redrawSpinner').set_visible(processing)
    builder.get_object('redrawButton').set_visible(not processing)

    builder.get_object('topbarContainer').set_sensitive(not processing)
    builder.get_object('toolbarContainer').set_sensitive(not processing)


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

    def do_wrap_month(self, widget):
        month = int(widget.get_text())
        if month == 1:
            builder.get_object('yearButton').spin(Gtk.SpinType.STEP_FORWARD, 1)
        else:
            builder.get_object('yearButton').spin(Gtk.SpinType.STEP_BACKWARD, 1)


with DB.connect() as connection:
    with connection.cursor(DictCursor) as cursor:
        GObject.threads_init()

        builder = Gtk.Builder()
        builder.add_from_file('glade/timeline.glade')
        builder.connect_signals(Signals())
        window = builder.get_object('window')

        fig = Figure()

        canvas = FigureCanvas(fig)
        builder.get_object('plotContainer').add(canvas)

        toolbar = PlotToolbar(canvas, window)
        builder.get_object('toolbarContainer').add(toolbar)

        window.show_all()
        set_processing(False)

        draw_figure()
        Gtk.main()
