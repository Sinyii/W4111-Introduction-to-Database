'''
HW4 will add checking the Redis cache to a simple, standalone  𝑟𝑒𝑡𝑟𝑖𝑒𝑣𝑒_𝑏𝑦_𝑡𝑒𝑚𝑝𝑙𝑎𝑡𝑒()  function.
Write a function  𝑎𝑑𝑑_𝑡𝑜_𝑐𝑎𝑐ℎ𝑒()  that adds a retrieve result to the cache.
Write a function  𝑟𝑒𝑡𝑟𝑖𝑒𝑣𝑒_𝑓𝑟𝑜𝑚_𝑐𝑎𝑐ℎ𝑒()  that checks the cache and returns the value if present.
Modify  𝑟𝑒𝑡𝑟𝑖𝑒𝑣𝑒_𝑏𝑦_𝑡𝑒𝑚𝑝𝑙𝑎𝑡𝑒()  to use the cache.
Check and return if cached.
Call DB and add to cache if not cached.
'''

import pymysql.cursors

class MysqlHelpers:

    def __init__(self):
        self.db_schema = None                                # Schema containing accessed data
        self.cnx = None                                      # DB connection to use for accessing the data.
        self.key_delimiter = '_'                             # This should probably be a config option.
        self.set_config()


    def get_new_connection(self, params=None):
        if not params:
            params = self.default_db_params

        cnx = pymysql.connect(
            host=params["dbhost"],
            port=params["port"],
            user=params["dbuser"],
            password=params["dbpw"],
            db=params["dbname"],
            charset=params["charset"],
            cursorclass=params["cursorClass"])
        return cnx

    def set_config(self):
        """
        Creates the DB connection and sets the global variables.

        :param cfg: Application configuration data.
        :return: None
        """



        db_params = {
            "dbhost": "localhost",
            "port": 3306,
            "dbname": "lahman2017",
            "dbuser": "dbuser",
            "dbpw": "dbuserdbuser",
            "cursorClass": pymysql.cursors.DictCursor,
            "charset": 'utf8mb4'
        }

        self.db_schema = "lahman2017"

        self.cnx = self.get_new_connection(db_params)

        print("Mysql Connection Object: ", self.cnx)
        return self.cnx


    # Given one of our magic templates, forms a WHERE clause.
    # { a: b, c: d } --> WHERE a=b and c=d. Currently treats everything as a string.
    # We can fix this by using PyMySQL connector query templates.
    def templateToWhereClause(self, t):
        s = ""
        for k,v in t.items():
            if s != "":
                s += " AND "
            s += k + "=\'" + v + "\'"

        if s != "":
            s = "WHERE " + s + ";"

        return s


    def run_q(self, cnx, q, args, fetch=False, commit=True):
        """
        :param cnx: The database connection to use.
        :param q: The query string to run.
        :param args: Parameters to insert into query template if q is a template.
        :param fetch: True if this query produces a result and the function should perform and return fetchall()
        :return:
        """
        #debug_message("run_q: q = " + q)
        #ut.debug_message("Q = " + q)
        #ut.debug_message("Args = ", args)

        result = None

        try:
            cursor = cnx.cursor()
            result = cursor.execute(q, args)
            if fetch:
                result = cursor.fetchall()
            if commit:
                cnx.commit()
        except Exception as original_e:
            #print("dffutils.run_q got exception = ", original_e)
            raise(original_e)

        return result

    def find_by_template(self, table, t, fields=None, limit=None, offset=None, orderBy=None):

        if t is not None:
            w = self.templateToWhereClause(t)
        else:
            w = ""

        if orderBy is not None:
            o = "order by " + ",".join(orderBy['fields']) + " " + orderBy['direction'] + " "
        else:
            o = ""

        if limit is not None:
            w += " LIMIT " + str(limit)
        if offset is not None:
            w += " OFFSET " + str(offset)

        if fields is None:
            fields = " * "
        else:
            fields = " " + ",".join(fields) + " "

        #cursor = self.cnx.cursor()
        q = "SELECT " + fields + " FROM " + table + " " + w + ";"

        r = self.run_q(self.cnx, q, None, fetch=True, commit=True)

        return r