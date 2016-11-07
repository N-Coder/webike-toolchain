from iss4e.util import BraceMessage as __
from matplotlib.ticker import FuncFormatter, MultipleLocator

from webike.ui.Grapher import Grapher


class DensityGrapher(Grapher):
    def get_data_async(self, imei, begin, end):
        self.cursor.execute(
            """SELECT YEAR(Stamp) AS year, MONTH(Stamp) AS month, COUNT(Stamp) AS count
            FROM imei{imei} imei
            GROUP BY year, month
            ORDER BY year, month ASC"""
                .format(imei=imei))
        return self.cursor.fetchall()

    def draw_figure_async(self, imei, begin, end, *data):
        if len(data) >= 1 and (data[0]['year'] < 2000 or data[0]['month'] < 1):
            self.logger.warning(__("IMEI {} has NULL row: {}", imei, data[0]))
        counts = [r for r in data if r['year'] > 0 and r['month'] > 0]

        self.fig.clear()
        ax = self.fig.add_subplot(111)

        ax.bar(
            left=[r['year'] * 12 + r['month'] - 1 for r in counts],
            height=[r['count'] for r in counts],
            width=1, align='center'
        )

        ax.set_title("Data Density for {}".format(imei))
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: str(int(x // 12))))
        ax.xaxis.set_major_locator(MultipleLocator(12))
        ax.xaxis.set_minor_formatter(FuncFormatter(lambda x, pos: str(int(x % 12 + 1))))
        ax.xaxis.set_minor_locator(MultipleLocator(1))
        for tick in ax.xaxis.get_major_ticks():
            tick.label.set_ha('right')
            tick.label.set_rotation(45)
            tick.label.set_rotation_mode('default')
        self.fig.tight_layout()

    @classmethod
    def requires_month(cls):
        return False
