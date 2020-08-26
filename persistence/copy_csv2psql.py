import psycopg2
from logger import Logger
import time


class CopyCsv:

    def copy_from_file(self, conn, path):
        t = time.process_time()
        log = Logger()
        f = open(path, 'r')
        cursor = conn.cursor()

        try:
            copy_sql = """
            COPY daily from stdin WITH CSV HEADER
            DELIMITER as ','
            """
            cursor.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            elapsed_t = time.process_time() - t
            log.log('Copied succesfully to postgresql', elapsed_t)

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            elapsed_t = time.process_time() - t
            log.log("Error: %s" % error, elapsed_t)
            conn.rollback()
            cursor.close()
            return

        cursor.close()
        elapsed_t = time.process_time() - t

