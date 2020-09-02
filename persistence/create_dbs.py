from logger import Logger
import time


class CreateDb(Logger):

    def create_table(self, conn):
        obj_t = Logger()
        t = time.process_time()
        cursor = conn.cursor()

        cursor.execute("""
                DROP TABLE IF EXISTS daily;
                CREATE UNLOGGED TABLE daily (
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
                        DROP TABLE IF EXISTS estados;
                        CREATE UNLOGGED TABLE estados (
                            clave_entidad  TEXT,
                            entidad_federativa  TEXT,
                            abreviatura  TEXT
                        );
                    """)
        cursor.execute("""
                                DROP TABLE IF EXISTS resultado;
                                CREATE UNLOGGED TABLE resultado (
                                    clave  TEXT,
                                    descripcion  TEXT
                                );
                            """)
        cursor.execute("""
                                        DROP TABLE IF EXISTS nacionalidad;
                                        CREATE UNLOGGED TABLE nacionalidad (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        DROP TABLE IF EXISTS tipopaciente;
                                        CREATE UNLOGGED TABLE tipopaciente (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        DROP TABLE IF EXISTS sexo;
                                        CREATE UNLOGGED TABLE sexo (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        DROP TABLE IF EXISTS sector;
                                        CREATE UNLOGGED TABLE sector (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        DROP TABLE IF EXISTS origen;
                                        CREATE UNLOGGED TABLE origen (
                                            clave  TEXT,
                                            descripcion  TEXT
                                        );
                                    """)
        cursor.execute("""
                                        DROP TABLE IF EXISTS municipios;
                                        CREATE UNLOGGED TABLE municipios (
                                            clave_municipio  TEXT,
                                            municipio  TEXT,
                                            clave_entidad TEXT
                                        );
                                    """)
        conn.commit()
        conn.close()
        elapsed_t = time.process_time() - t
        obj_t.log('Tablas creadas', elapsed_t)
