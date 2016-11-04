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
        class : logging.handlers.FileHandler
        formatter: default
        level: DEBUG
        filename: python-main.log
      rotating-file:
        class : logging.handlers.TimedRotatingFileHandler
        formatter: default
        level: DEBUG
        filename: python-main-r.log
        when: midnight
        backupCount: 3
    loggers:
      root:
        level: DEBUG
        handlers: [console, file]
      urllib3:
        level: WARNING
      requests:
        level: WARNING
    formatters:
      default:
        format: "%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s"
    """)
    logging.config.dictConfig(logging_config)
