import redis
import json
from redis_helper import MysqlHelpers
from urllib.parse import urlencode

"""
Connect to local Redis server. StrictRedis complies more closely with standard than
simple Redis client in this package. decode_responses specifies whether or not to convert
bytes to UTF8 character string or treat as raw bytes, e.g. an image, audio stream, etc.
"""


class RedisFunctions:

    def __init__(self):
        self.r = redis.StrictRedis(
            host='localhost',
            port=6379,
            charset="utf-8", decode_responses=True)

        if self.r:
            print("Connected with Redis.")

        self.db = MysqlHelpers()

    # ----- sh3907 implementation begins----- #
    # requirement function 1
    def retrieve_by_template(self, table, template, fields=None, limit=None, offset=None, order_by=None,
                             use_cache=False):

        in_cache = None

        if use_cache:

            in_cache = self.check_cache(table, template, fields, limit, offset, order_by)

            if in_cache:
                print("Check cache returned: ", json.dumps(json.loads(in_cache), indent=2))
                print("CACHE HIT")

        if not use_cache or not in_cache:
            q_result = self.db.find_by_template(table, template, fields, limit, offset, order_by)

            if q_result:
                print("Retrieve data from mysql database: ", json.dumps(q_result, indent=2))
                self.add_to_cache(table, template, fields, limit, offset, order_by, q_result)

                if use_cache:
                    print("CACHE MISS")
            else:
                print("Data not found from the database. Please modify your input")

    # requirement function 2
    def retrieve_from_cache(self, key):
        """
        :param key: A valid Redis key.
        :return: The "map object" associated with the key.
        """
        result = self.r.get(key)
        return result

    # requirement function 3
    def add_to_cache(self, table, tmp, fields, limit, offset, order_by, q_result):
        key = self.generate_key(table, tmp, fields, limit, offset, order_by)
        v = json.dumps(q_result)
        save_result = self.r.set(key, v)

        if save_result:
            print("Successful add key={} data into cache".format(key))
        else:
            print("Fail to add into cache.")

    def generate_key(self, table, template, fields, limit, offset, order_by):
        tmp = template.copy()
        if fields:
            tmp['field'] = ",".join(fields)
        if limit:
            tmp['limit'] = limit
        if offset:
            tmp['offset'] = offset
        if order_by:
            tmp['order_by'] = order_by
        k = urlencode(tmp)
        k = table + "/" + k

        return k

    def check_cache(self, table, tmp, fields, limit, offset, order_by):
        key = self.generate_key(table, tmp, fields, limit, offset, order_by)
        result = self.retrieve_from_cache(key)
        if result:
            return result
        else:
            return None

    def delete_keys(self):
        keys = self.get_keys()
        self.r.delete(*keys)

    # ----- sh3907 implementation ends----- #

    def get_keys(self):
        result = self.r.keys()
        return result
