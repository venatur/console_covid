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
                    fecha_actualizacion  TEXT,
                    id_registro  TEXT,
                    origen  TEXT,
                    sector TEXT,
                    entidad_um TEXT,
                    sexo  TEXT,
                    entidad_nac TEXT,
                    entidad_res TEXT,
                    municipio_res TEXT,
                    tipo_paciente TEXT,
                    fecha_ingreso TEXT,
                    fecha_sintomas TEXT,
                    fecha_def TEXT,
                    intubado TEXT,
                    neumonia TEXT,
                    edad TEXT,
                    nacionalidad TEXT,
                    embarazo TEXT,
                    habla_lengua_indig TEXT,
                    diabetes TEXT,
                    epoc TEXT,
                    asma TEXT,
                    inmusupr TEXT,
                    hipertension TEXT,
                    otra_com TEXT,
                    cardiovascular TEXT,
                    obesidad TEXT,
                    renal_cronica TEXT,
                    tabaquismo TEXT,
                    otro_caso TEXT,
                    resultado TEXT,
                    migrante TEXT,
                    pais_nacionalidad TEXT,
                    pais_origen TEXT,
                    uci TEXT
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
