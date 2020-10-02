from logger import Logger
import time
from persistence.extra_vars import getFecha
"""class with method to copy data to daily table on postgres database"""


class SaveNuevos:

    def save_news(self, diarios, fecha):
        sqlQuery = "insert into nuevos_count " \
                   "(diarios, fecha) values ( " \
                   "%s, %s)"
        return sqlQuery

    def SaveData(self, conn):
        """receives connection variable and path of csv file, inserts row by row data to database
        function(conn[connector from postgres_connect class], path[path of csv file]) -> insert rows
        and change values before insertions
        """
        t = time.process_time()
        log = Logger()
        cursor = conn.cursor()
        fecha = getFecha(cursor)
        sqlQuery = "insert into nuevos " \
                   "select * from new_data " \
                   "where not exists( " \
                   "select * from old_data " \
                   "where new_data.id_registro=old_data.id_registro)"

        cursor.execute(sqlQuery)
        conn.commit()
        elapsed_t = time.process_time() - t
        log.log('Datos de cambios copiados a tabla Nuevos', elapsed_t)