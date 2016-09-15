from __future__ import division

import os

import pymysql as MySQLdb


# only need one instance of this connector to manage all database calls
class databaseConnector(object):
    def __init__(self):
        try:
            # now go open that conf file and get the username and password

            self.myDB = MySQLdb.connect(host="tornado.cs.uwaterloo.ca",
                                        port=3306,
                                        user=os.environ['MYSQL_USER'],
                                        passwd=os.environ['MYSQL_PASSWORD'],
                                        db="webike")
            # http://legacy.python.org/dev/peps/pep-0249/#id7
            self.cur = self.myDB.cursor()
        except MySQLdb.Error as msg:
            print("MYSQL ERROR!")
            print(msg)

    def shutDown(self):
        self.cur.close()  # close the cursor
        self.myDB.close()

    # execute a select query and return results as a generator
    def SQLSelectGenerator(self, stmt):
        # print("Executing: " + stmt)
        try:
            self.cur.execute(stmt)
        except MySQLdb.Error as msg:
            print("MYSQL ERROR: {0}".format(msg))

        data = self.cur.fetchall()
        for row in data:
            yield row

        # TO DO ONE AT A TIME:
        """row = ""
        while row is not None:
            row = self.cur.fetchone()
            yield row
            
        #this also seems to be valid though it is cryptic:
        for row in self.cur:
            yield row"""

    # execute a bunch of sql queries
    def SQLExecQueryQueue(self, Q):
        for q in Q:
            try:
                self.cur.execute(q)
            except MySQLdb.Error as msg:
                print("MYSQL ERROR!:{0}".format(msg))  # print error but keep going
        self.myDB.commit()

        # exectute a single query

    def SQLExecQuery(self, stmt):
        try:
            print(stmt)
            self.cur.execute(stmt)
            self.myDB.commit()
        except MySQLdb.Error as msg:
            print("MYSQL ERROR!")
            print(msg)
