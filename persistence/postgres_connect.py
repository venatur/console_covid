import psycopg2
import sys
from config import Config


class Connection:

    def __init__(self):
        self.params = Config.PARAMS_DIC

    def __str__(self):
        return "Connection Established"

    def connect(self):

        try:
            conn = psycopg2.connect(**self.params)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit(1)

        return conn
