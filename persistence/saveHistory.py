from logger import Logger
import time
"""class with method to copy data to daily table on postgres database"""


class HistoryData:

    def SaveData(self, conn):
        """receives connection variable and path of csv file, insterts row by row data to database
        function(conn[connector from postgres_connect class], path[path of csv file]) -> instert rows
        and change values before insertions
        """
        t = time.process_time()
        log = Logger()
        cursor = conn.cursor()
        sqlQuery = "insert into historia " \
                   "select * from new_data"
        cursor.execute(sqlQuery)
        conn.commit()
        conn.close()
        elapsed_t = time.process_time() - t
        log.log('Datos de cambios copiados a tabla', elapsed_t)