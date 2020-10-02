from logger import Logger
import time
"""class with method to copy data to daily table on postgres database"""


class SaveCountNuevos:

    def SaveData(self, conn):
        """receives connection variable and path of csv file, insterts row by row data to database
        function(conn[connector from postgres_connect class], path[path of csv file]) -> instert rows
        and change values before insertions
        """
        t = time.process_time()
        log = Logger()
        cursor = conn.cursor()
        sqlQuery = "insert into nuevos_count " \
                   "select distinct count(*), fecha_actualizacion from nuevos "\
                   "group by fecha_actualizacion "\
                    "order by fecha_actualizacion"
        cursor.execute(sqlQuery)
        conn.commit()
        conn.close()
        elapsed_t = time.process_time() - t
        log.log('Datos de nuevos registros copiados a tabla', elapsed_t)