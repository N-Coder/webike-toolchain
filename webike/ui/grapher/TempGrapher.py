import numpy as np
from iss4e.util.math import smooth
from matplotlib import dates as mdates

from webike.ui.Grapher import Grapher


class TempGrapher(Grapher):
    def get_data_async(self, imei, begin, end):
        self.cursor.execute(
            """SELECT Stamp, TempBattery, TempBox, AtmosPress
            FROM imei{imei} imei
            WHERE Stamp >= '{min}' AND Stamp <= '{max}'
            ORDER BY Stamp ASC"""
                .format(imei=imei, min=begin, max=end))
        charge_values = self.cursor.fetchall()
        charge_values = smooth(charge_values, 'TempBattery', alpha=0.75)
        charge_values = smooth(charge_values, 'TempBox', alpha=0.75)
        charge_values = list(charge_values)  # smooth returns an iterator, this forces generation of all elements
        return charge_values

    def draw_figure_async(self, imei, begin, end, *data):
        temp = data

        legend = self.fig.add_subplot(111).legend_
        legend_visible = not legend or legend.get_visible()
        self.fig.clear()
        ax = self.fig.add_subplot(111)

        ax.plot(
            list([x['Stamp'] for x in temp]),
            list([x['TempBattery_smooth'] or np.nan for x in temp]),
            'b-', label="Battery Temperature °C", alpha=0.9
        )
        ax.plot(
            list([x['Stamp'] for x in temp]),
            list([x['TempBox_smooth'] or np.nan for x in temp]),
            'g-', label="Box Temperature °C", alpha=0.9
        )
        ax.plot(
            list([x['Stamp'] for x in temp]),
            list([x['AtmosPress'] / 1000 * 30 if x['AtmosPress'] else np.nan for x in temp]),
            'r-', label="Pressure", alpha=0.9
        )

        legend = ax.legend(loc='upper right')
        legend.set_visible(legend_visible)

        ax.set_title("{} -- {}-{}".format(imei, begin.year, begin.month))
        ax.set_xlim(begin, end)
        ax.set_ylim(-10, 30)
        ax.xaxis.set_major_locator(mdates.DayLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d'))
        ax.fmt_xdata = mdates.DateFormatter('%d. %H:%M.%S')
        self.fig.tight_layout()
