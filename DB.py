import sys
import time

from pymysql.cursors import Cursor, DictCursorMixin


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
            print("Took {:.2f}s for executing query affecting {} rows"
                  .format(time.perf_counter() - start, res))
            return res
        except:
            print("Query failed after {:.2f}s:\n{}".format(time.perf_counter() - start, q), file=sys.stderr)
            raise


class QualifiedDictCursor(QualifiedDictCursorMixin, StopwatchCursorMixin, Cursor):
    """A cursor which returns results as a dictionary with keys always consisting of the fully qualified column name"""


class DictCursor(DictCursorMixin, StopwatchCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""
