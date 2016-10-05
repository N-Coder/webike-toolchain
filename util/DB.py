import configparser
import logging
import os
import time
import warnings
from contextlib import contextmanager

import pymysql
from pymysql.cursors import Cursor, DictCursor as _DictCursor, SSDictCursor as _SSDictCursor

from util.DBStopwatch import StopwatchConnection as _Connection
from util.Logging import BraceMessage as __

# for Connection without Stopwatch:
# from pymysql.connections import _Connection

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


class QualifiedDictCursorMixin(object):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super(QualifiedDictCursorMixin, self)._do_get_result()
        fields = []
        if self.description:
            for f in self._result.fields:
                fields.append(f.table_name + '.' + f.name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self.dict_type(zip(self._fields, row))


class QualifiedDictCursor(QualifiedDictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary with keys always consisting of the fully qualified column name"""


DictCursor = _DictCursor
StreamingDictCursor = _SSDictCursor
Connection = _Connection


def default_credentials():
    cred = {
        'host': "tornado.cs.uwaterloo.ca",
        'port': 3306,
        'user': None,
        'passwd': None,
        'db': "webike"
    }

    cred_env = {
        'host': 'WEBIKE_DB_HOST',
        'port': 'WEBIKE_DB_PORT',
        'user': 'WEBIKE_DB_USER',
        'passwd': 'WEBIKE_DB_PASS',
        'db': 'WEBIKE_DB_NAME'
    }
    cred.update(dict([(k, os.environ[v]) for k, v in cred_env.items() if v in os.environ]))

    parser = configparser.ConfigParser()
    conf_files = ["config.ini", "instance/config.ini", os.path.expanduser("~/iss4e_config.ini"),
                  os.path.expanduser("~/.iss4e_config.ini")]
    valid_confs = parser.read(conf_files)
    if len(valid_confs) > 0:
        cred.update(dict([(k, v) for k, v in parser.items('WeBike-DB') if k in cred]))
        logger.debug(__("Read config from files: {}", valid_confs))
    else:
        logger.debug(__("No valid config files found, read config from environment"))

    if not (cred['user'] and cred['passwd']):
        logger.warning(__("Could not find DB username or password. Searched files {} and environment vars.",
                          conf_files))
        cred['user'] = cred['user'] or ""
        cred['passwd'] = cred['passwd'] or ""

    cred['port'] = int(cred['port'])
    return cred


@contextmanager
def connect(credentials=default_credentials()):
    warnings.filterwarnings('error', category=pymysql.Warning)
    credentials['port'] = int(credentials['port'])
    connection = _Connection(**credentials)
    start = time.perf_counter()
    try:
        yield connection
    except:
        connection.rollback()
        raise
    finally:
        dur = time.perf_counter() - start
        logger.debug(__("DB connection open for {:.2f}s", dur))
        connection.close()
