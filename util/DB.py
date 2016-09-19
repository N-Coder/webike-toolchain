import logging
import os
import time
import warnings
from contextlib import contextmanager

import pymysql
from pymysql.cursors import Cursor, DictCursorMixin

from util.Logging import BraceMessage as __

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


class StopwatchCursorMixin(object):
    def _query(self, q):
        start = time.perf_counter()
        try:
            res = super(StopwatchCursorMixin, self)._query(q)
            dur = time.perf_counter() - start
            if dur > 2:
                logger.debug(__("Took {:.2f}s for executing query affecting {} rows",
                                dur, res))
            return res
        except:
            logger.error(__("Query failed after {:.2f}s:\n{}", time.perf_counter() - start, q))
            raise


class QualifiedDictCursor(QualifiedDictCursorMixin, StopwatchCursorMixin, Cursor):
    """A cursor which returns results as a dictionary with keys always consisting of the fully qualified column name"""


class DictCursor(DictCursorMixin, StopwatchCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""


@contextmanager
def connect():
    warnings.filterwarnings('error', category=pymysql.Warning)
    connection = pymysql.connect(
        host="tornado.cs.uwaterloo.ca",
        port=3306,
        user=os.environ['MYSQL_USER'],
        passwd=os.environ['MYSQL_PASSWORD'],
        db="webike"
    )
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
