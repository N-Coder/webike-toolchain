import logging
import os
import time
import warnings
from contextlib import contextmanager

import pymysql
from pymysql import connections
from pymysql.constants import COMMAND
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


class QualifiedDictCursor(QualifiedDictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary with keys always consisting of the fully qualified column name"""


class DictCursor(DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""


class StopwatchConnection(connections.Connection):
    def __init__(self, *args, **kwargs):
        connections.Connection.__init__(self, *args, **kwargs)
        self._query_start = 0
        self.result_class = StopwatchMySQLResult

    def query(self, sql, unbuffered=False):
        self._query_start = time.perf_counter()
        try:
            res = connections.Connection.query(self, sql, unbuffered)
            dur = time.perf_counter() - self._query_start
            if not unbuffered and dur > 2:
                logger.debug(__("Took {:.2f}s for executing query affecting {:,} rows",
                                dur, res))
            return res
        except:
            logger.error(__("Query failed after {:.2f}s:\n{}", time.perf_counter() - self._query_start, sql))
            raise
        finally:
            self._query_start = 0

    def show_warnings(self):
        """SHOW WARNINGS"""
        self._execute_command(COMMAND.COM_QUERY, "SHOW WARNINGS")
        result = self.result_class(self)
        result.read()
        return result.rows

    def _read_query_result(self, unbuffered=False):
        if unbuffered:
            try:
                result = self.result_class(self)
                result.init_unbuffered_query()
            except:
                result.unbuffered_active = False
                result.connection = None
                raise
        else:
            result = self.result_class(self)
            result.read()
        self._result = result
        if result.server_status is not None:
            self.server_status = result.server_status
        return result.affected_rows


class StopwatchMySQLResult(connections.MySQLResult):
    def init_unbuffered_query(self):
        self.unbuffered_active = True
        first_packet = self.connection._read_packet()
        if (time.perf_counter() - self.connection._query_start) > 2:
            logger.debug("Server took {:.2f}s for processing request"
                         .format(time.perf_counter() - self.connection._query_start))

        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        elif first_packet.is_load_local_packet():
            self._read_load_local_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        else:
            self.field_count = first_packet.read_length_encoded_integer()
            self._get_descriptions()

            # Apparently, MySQLdb picks this number because it's the maximum
            # value of a 64bit unsigned integer. Since we're emulating MySQLdb,
            # we set it to this instead of None, which would be preferred.
            self.affected_rows = 18446744073709551615

    def read(self):
        try:
            first_packet = self.connection._read_packet()
            if (time.perf_counter() - self.connection._query_start) > 2:
                logger.debug("Server took {:.2f}s for processing request"
                             .format(time.perf_counter() - self.connection._query_start))

            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            elif first_packet.is_load_local_packet():
                self._read_load_local_packet(first_packet)
            else:
                self._read_result_packet(first_packet)
        finally:
            self.connection = None

    def _read_rowdata_packet(self):
        """Read a rowdata packet for each data row in the result set."""
        rows = []
        last_print = time.perf_counter()
        last_rows = 0
        while True:
            packet = self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                self.connection = None  # release reference to kill cyclic reference.
                break
            rows.append(self._read_row_from_packet(packet))
            if (len(rows) % 2000) == 0 and (time.perf_counter() - last_print) > 5:
                logger.debug("Got {:,} rows after {:.2f}s ({:,.2f} rows per second)"
                             .format(len(rows), time.perf_counter() - self.connection._query_start,
                                     (len(rows) - last_rows) / (time.perf_counter() - last_print)))
                last_print = time.perf_counter()
                last_rows = len(rows)

        self.affected_rows = len(rows)
        self.rows = tuple(rows)


@contextmanager
def connect():
    warnings.filterwarnings('error', category=pymysql.Warning)
    connection = StopwatchConnection(
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
