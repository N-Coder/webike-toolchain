import os
import warnings

import pymysql
from pymysql.cursors import Cursor


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


connection = pymysql.connect(
    host="tornado.cs.uwaterloo.ca",
    port=3306,
    user=os.environ['MYSQL_USER'],
    passwd=os.environ['MYSQL_PASSWORD'],
    db="webike"
)
warnings.filterwarnings('error', category=pymysql.Warning)
cursor = connection.cursor(pymysql.cursors.DictCursor)
qcursor = connection.cursor(QualifiedDictCursor)
