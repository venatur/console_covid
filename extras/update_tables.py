from logger import Logger
import time
from persistence.postgres_connect import Connection


class UpdateDb(Logger):
    def __init__(self):
        self.objc = Connection()
        conn = self.objc.connect()
        self.create_table(conn)

    def update_table(self, conn):
        obj_t = Logger()
        t = time.process_time()
        cursor = conn.cursor()

        cursor.execute("""ALTER TABLE new_data
    ADD COLUMN indigena INTEGER ,
    add column toma_muestra INTEGER ,
    add column clasificacion_final INTEGER
            """)
        cursor.execute("""ALTER TABLE cambios
            ADD COLUMN indigena INTEGER ,
            add column toma_muestra INTEGER ,
            add column clasificacion_final INTEGER
                    """)
        cursor.execute("""ALTER TABLE historia
            ADD COLUMN indigena INTEGER ,
            add column toma_muestra INTEGER ,
            add column clasificacion_final INTEGER
                    """)
        cursor.execute("""ALTER TABLE nuevos
            ADD COLUMN indigena INTEGER ,
            add column toma_muestra INTEGER ,
            add column clasificacion_final INTEGER
                    """)
        cursor.execute("""ALTER TABLE old_data
            ADD COLUMN indigena INTEGER ,
            add column toma_muestra INTEGER ,
            add column clasificacion_final INTEGER
                    """)
        cursor.execute("""ALTER TABLE new_data 
    rename COLUMN resultado to resultado_lab
                            """)
        cursor.execute("""ALTER TABLE cambios 
            rename COLUMN resultado to resultado_lab
                                    """)
        cursor.execute("""ALTER TABLE historia 
            rename COLUMN resultado to resultado_lab
                                    """)
        cursor.execute("""ALTER TABLE nuevos 
            rename COLUMN resultado to resultado_lab
                                    """)

        conn.commit()
        conn.close()
        elapsed_t = time.process_time() - t
        obj_t.log('Tablas actualizados', elapsed_t)


obj = UpdateDb()