from logger import Logger
import time
from persistence.postgres_connect import Connection


class CreateDb(Logger):
    def __init__(self):
        self.objc = Connection()
        conn = self.objc.connect()
        self.create_table(conn)

    def create_table(self, conn):
        obj_t = Logger()
        t = time.process_time()
        cursor = conn.cursor()

        cursor.execute("""
                DROP TABLE IF EXISTS new_data;
                CREATE UNLOGGED TABLE new_data (
                    fecha_actualizacion  DATE ,
                    id_registro  TEXT,
                    origen  INTEGER ,
                    sector INTEGER,
                    entidad_um INTEGER,
                    sexo  INTEGER,
                    entidad_nac INTEGER,
                    entidad_res INTEGER,
                    municipio_res INTEGER,
                    tipo_paciente INTEGER,
                    fecha_ingreso DATE,
                    fecha_sintomas DATE,
                    fecha_def DATE,
                    intubado INTEGER,
                    neumonia INTEGER,
                    edad INTEGER,
                    nacionalidad INTEGER,
                    embarazo INTEGER,
                    habla_lengua_indig INTEGER,
                    diabetes INTEGER,
                    epoc INTEGER,
                    asma INTEGER,
                    inmusupr INTEGER,
                    hipertension INTEGER,
                    otra_com INTEGER,
                    cardiovascular INTEGER,
                    obesidad INTEGER,
                    renal_cronica INTEGER,
                    tabaquismo INTEGER,
                    otro_caso INTEGER,
                    resultado INTEGER,
                    migrante INTEGER,
                    pais_nacionalidad TEXT,
                    pais_origen TEXT,
                    uci INTEGER
                );
            """)
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS old_data
                        (
                           fecha_actualizacion  DATE ,
                           id_registro  TEXT,
                           origen  INTEGER ,
                           sector INTEGER,
                           entidad_um INTEGER,
                           sexo  INTEGER,
                           entidad_nac INTEGER,
                           entidad_res INTEGER,
                           municipio_res INTEGER,
                           tipo_paciente INTEGER,
                           fecha_ingreso DATE,
                           fecha_sintomas DATE,
                           fecha_def DATE,
                           intubado INTEGER,
                           neumonia INTEGER,
                           edad INTEGER,
                           nacionalidad INTEGER,
                           embarazo INTEGER,
                           habla_lengua_indig INTEGER,
                           diabetes INTEGER,
                           epoc INTEGER,
                           asma INTEGER,
                           inmusupr INTEGER,
                           hipertension INTEGER,
                           otra_com INTEGER,
                           cardiovascular INTEGER,
                           obesidad INTEGER,
                           renal_cronica INTEGER,
                           tabaquismo INTEGER,
                           otro_caso INTEGER,
                           resultado INTEGER,
                           migrante INTEGER,
                           pais_nacionalidad TEXT,
                           pais_origen TEXT,
                           uci INTEGER
                       );
                   """)
        cursor.execute("""
                               CREATE TABLE IF NOT EXISTS cambios
                                (
                                   id_registro  TEXT,
                                   origen  INTEGER ,
                                   sector INTEGER,
                                   entidad_um INTEGER,
                                   sexo  INTEGER,
                                   entidad_nac INTEGER,
                                   entidad_res INTEGER,
                                   municipio_res INTEGER,
                                   tipo_paciente INTEGER,
                                   fecha_ingreso INTEGER,
                                   fecha_sintomas INTEGER,
                                   fecha_def INTEGER,
                                   intubado INTEGER,
                                   neumonia INTEGER,
                                   edad INTEGER,
                                   nacionalidad INTEGER,
                                   embarazo INTEGER,
                                   habla_lengua_indig INTEGER,
                                   diabetes INTEGER,
                                   epoc INTEGER,
                                   asma INTEGER,
                                   inmusupr INTEGER,
                                   hipertension INTEGER,
                                   otra_com INTEGER,
                                   cardiovascular INTEGER,
                                   obesidad INTEGER,
                                   renal_cronica INTEGER,
                                   tabaquismo INTEGER,
                                   otro_caso INTEGER,
                                   resultado INTEGER,
                                   migrante INTEGER,
                                   pais_nacionalidad INTEGER,
                                   pais_origen INTEGER,
                                   uci INTEGER
                               );
                           """)
        cursor.execute("""
                                       CREATE TABLE IF NOT EXISTS nuevos
                                        (
                                           fecha_actualizacion  DATE ,
                                            id_registro  TEXT,
                                            origen  INTEGER ,
                                            sector INTEGER,
                                            entidad_um INTEGER,
                                            sexo  INTEGER,
                                            entidad_nac INTEGER,
                                            entidad_res INTEGER,
                                            municipio_res INTEGER,
                                            tipo_paciente INTEGER,
                                            fecha_ingreso DATE,
                                            fecha_sintomas DATE,
                                            fecha_def DATE,
                                            intubado INTEGER,
                                            neumonia INTEGER,
                                            edad INTEGER,
                                            nacionalidad INTEGER,
                                            embarazo INTEGER,
                                            habla_lengua_indig INTEGER,
                                            diabetes INTEGER,
                                            epoc INTEGER,
                                            asma INTEGER,
                                            inmusupr INTEGER,
                                            hipertension INTEGER,
                                            otra_com INTEGER,
                                            cardiovascular INTEGER,
                                            obesidad INTEGER,
                                            renal_cronica INTEGER,
                                            tabaquismo INTEGER,
                                            otro_caso INTEGER,
                                            resultado INTEGER,
                                            migrante INTEGER,
                                            pais_nacionalidad TEXT,
                                            pais_origen TEXT,
                                            uci INTEGER
                                       );
                                   """)
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS estados (
                            clave_entidad  TEXT,
                            entidad_federativa  TEXT,
                            abreviatura  TEXT
                        );
                    """)
        cursor.execute("""
                                CREATE TABLE IF NOT EXISTS resultado (
                                    clave  TEXT,
                                    descripcion  TEXT
                                );
                            """)
        cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS nacionalidad (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS tipopaciente (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS sexo(
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS sector (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS origen (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                                CREATE TABLE IF NOT EXISTS registros (
                                                    diarios  INTEGER,
                                                    fecha  DATE
                                                );
                                            """)
        cursor.execute("""
                                                        CREATE TABLE IF NOT EXISTS nuevos_count (
                                                            diarios  INTEGER,
                                                            fecha  DATE
                                                        );
                                                    """)

        conn.commit()
        conn.close()
        elapsed_t = time.process_time() - t
        obj_t.log('Tablas creadas', elapsed_t)


obj = CreateDb()