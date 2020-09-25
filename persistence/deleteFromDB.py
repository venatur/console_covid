from logger import Logger
import time
from persistence.postgres_connect import Connection


class DeleteFromDB():

    def __init__(self):
        self.objc = Connection()
        self.conn = self.objc.connect()
        self.cursor = self.conn.cursor()
        self.log = Logger()

    def delete_new_data(self):
        t = time.process_time()
        sql = "delete from new_data"
        self.cursor.execute(sql)
        self.conn.commit()
        elapsed_t = time.process_time() - t
        self.log.log('datos borrados de tabla newdata', elapsed_t)

    def delete_old_data(self):
        t = time.process_time()
        sql = "delete from old_data"
        self.cursor.execute(sql)
        self.conn.commit()
        elapsed_t = time.process_time() - t
        self.log.log('datos borrados de tabla old_data', elapsed_t)

    def delete_nuevos(self):
        t = time.process_time()
        sql = "delete from nuevos"
        self.cursor.execute(sql)
        self.conn.commit()
        elapsed_t = time.process_time() - t
        self.log.log('datos borrados de tabla nuevos', elapsed_t)

    def delete_cambios(self):
        t = time.process_time()
        sql = "delete from cambios"
        self.cursor.execute(sql)
        self.conn.commit()
        elapsed_t = time.process_time() - t
        self.log.log('datos borrados de tabla cambios', elapsed_t)

    def delete_registros(self):
        t = time.process_time()
        sql = "delete from registros"
        self.cursor.execute(sql)
        self.conn.commit()
        elapsed_t = time.process_time() - t
        self.log.log('datos borrados de tabla registros', elapsed_t)
