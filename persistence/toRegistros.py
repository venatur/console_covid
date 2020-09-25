from logger import Logger
import time
from persistence.extra_vars import getFecha, get_numero


class ToRegistros:

    def copyToRegistros(self, conn):
        t = time.process_time()
        log = Logger()
        cursor = conn.cursor()
        fecha = getFecha(cursor)
        numero = get_numero(cursor)
        sqlQuery = "insert into registros " \
                   "(diarios, fecha) values ( " \
                   "%s, %s)"
        cursor.execute(sqlQuery, [numero[0], fecha[0]])
        conn.commit()
        conn.close()
        elapsed_t = time.process_time() - t
        log.log('Registro en Registros', elapsed_t)