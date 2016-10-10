import logging


class Grapher():
    logger = logging.getLogger(__name__)

    def __init__(self, callback, cursor, fig):
        self.callback = callback
        self.cursor = cursor
        self.fig = fig

    def __call__(self, imei, begin, end):
        self.logger.debug("enter get_data_async")
        data = self.get_data_async(imei, begin, end)
        self.logger.debug("leave get_data_async")

        self.logger.debug("enter draw_figure_async")
        self.draw_figure_async(imei, begin, end, *data)
        self.logger.debug("leave draw_figure_async")

        self.callback(imei, end, begin)

    def get_data_async(self, imei, begin, end):
        raise NotImplementedError()

    def draw_figure_async(self, imei, begin, end, *data):
        raise NotImplementedError()

    @classmethod
    def requires_month(cls):
        return True
