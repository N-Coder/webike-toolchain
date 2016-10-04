import numpy as np
from matplotlib import dates as mdates
from matplotlib import patches as mpatches

from ui.Grapher import Grapher
from util.Utils import smooth, discharge_curr_to_ampere


class ChargeGrapher(Grapher):
    def get_data_async(self, imei, begin, end):
        self.cursor.execute(
            """SELECT Stamp, ChargingCurr, DischargeCurr, soc.soc_smooth AS soc_smooth, soc_rie.soc_smooth AS soc_rie_smooth
            FROM imei{imei} imei
            JOIN webike_sfink.soc ON Stamp = soc.time AND soc.imei = '{imei}'
            JOIN webike_sfink.soc_rie ON Stamp = soc_rie.time AND soc_rie.imei = '{imei}'
            WHERE Stamp >= '{min}' AND Stamp <= '{max}'
            ORDER BY Stamp ASC"""
                .format(imei=imei, min=begin, max=end))
        charge_values = self.cursor.fetchall()
        charge_values = smooth(charge_values, 'ChargingCurr')
        charge_values = smooth(charge_values, 'DischargeCurr')
        charge_values = list(charge_values)  # smooth returns an iterator, this forces generation of all elements

        self.cursor.execute(
            "SELECT * FROM webike_sfink.charge_cycles "
            "WHERE imei='{imei}' AND end_time >= '{min}' AND start_time <= '{max}' "
            "ORDER BY start_time".format(imei=imei, min=begin, max=end))
        charge_cycles = self.cursor.fetchall()

        self.cursor.execute(
            "SELECT * FROM trip{imei} "
            "WHERE end_time >= '{min}' AND start_time <= '{max}' "
            "ORDER BY start_time ASC"
                .format(imei=imei, min=begin, max=end))
        trips = self.cursor.fetchall()

        return charge_values, charge_cycles, trips

    def draw_figure_async(self, imei, begin, end, *data):
        charge_values, charge_cycles, trips = data

        legend = self.fig.add_subplot(111).legend_
        legend_visible = not legend or legend.get_visible()
        self.fig.clear()
        ax = self.fig.add_subplot(111)

        ax.plot(
            list([x['Stamp'] for x in charge_values]),
            list([x['soc_smooth'] or np.nan for x in charge_values]),
            'b-', label="State of Charge [Box]", alpha=0.9
        )
        ax.plot(
            list([x['Stamp'] for x in charge_values]),
            list([x['soc_rie_smooth'] or np.nan for x in charge_values]),
            label="State of Charge [Riemann]", alpha=0.9, color='purple'
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
        ax.fmt_xdata = mdates.DateFormatter('%d. %H:%M.%S')
        self.fig.tight_layout()
