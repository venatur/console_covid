from logger import Logger
import time


class ToRegistros:

    def copyToRegistros(self, conn):
        t = time.process_time()
        log = Logger()
        cursor = conn.cursor()
        sql_query = "insert into registros " \
                    "select count(*) as numero, d2.fecha_actualizacion as fecha" \
                    "from new_data as d2 " \
                    "where not exists( " \
                    "select * from old_data as d " \
                    "where d.id_registro=d2.id_registro) "\
                    "group by d2.fecha_actualizacion"
        cursor.execute(sql_query)
        conn.commit()
        conn.close()
        elapsed_t = time.process_time() - t
        log.log('Registro en Registros', elapsed_t)