import logging
import logging.config

import yaml

__author__ = "Niko Fink"


class BraceMessage(object):
    def __init__(self, fmt, *args, **kwargs):
        self.fmt = fmt
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return self.fmt.format(*self.args, **self.kwargs)


def default_logging_config():
    # TODO load from file
    logging_config = yaml.load("""
    version: 1
    handlers:
      console:
        class: logging.StreamHandler
        formatter: default
        level: DEBUG
        stream: ext://sys.stdout
      file:
        class : logging.handlers.RotatingFileHandler
        formatter: default
        filename: logconfig.log
        maxBytes: 1024
        backupCount: 3
    loggers:
      urllib3:
        level: WARNING
      requests:
        level: WARNING
    formatters:
      default:
        format: "%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s"
    """)
    logging.config.dictConfig(logging_config)
