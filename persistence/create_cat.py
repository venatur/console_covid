import psycopg2
import time
from logger import Logger


class CreateCat:

    def crea_catalogo(self, conn):

        cursor = conn.cursor()
        t = time.process_time()
        log = Logger()
        try:
            path = 'res/catalogos/estados.csv'
            f = open(path, 'r', encoding="latin_1")
            copy_sql = """
            COPY estados from stdin WITH CSV HEADER
            DELIMITER as ','
            """
            cursor.copy_expert(sql=copy_sql, file=f)
            f.close()
            path = 'res/catalogos/municipios.csv'
            f = open(path, 'r', encoding="latin_1")
            copy_sql = """
                        COPY municipios from stdin WITH CSV HEADER
                        DELIMITER as ','
                        """
            cursor.copy_expert(sql=copy_sql, file=f)
            f.close()
            path = 'res/catalogos/nacionalidad.csv'
            f = open(path, 'r', encoding="latin_1")
            copy_sql = """
                        COPY nacionalidad from stdin WITH CSV HEADER
                        DELIMITER as ','
                        """
            cursor.copy_expert(sql=copy_sql, file=f)
            f.close()
            path = 'res/catalogos/origen.csv'
            f = open(path, 'r', encoding="latin_1")
            copy_sql = """
                        COPY origen from stdin WITH CSV HEADER
                        DELIMITER as ','
                        """
            cursor.copy_expert(sql=copy_sql, file=f)
            f.close()
            path = 'res/catalogos/resultado.csv'
            f = open(path, 'r', encoding="latin_1")
            copy_sql = """
                        COPY resultado from stdin WITH CSV HEADER
                        DELIMITER as ','
                        """
            cursor.copy_expert(sql=copy_sql, file=f)
            f.close()
            path = 'res/catalogos/sexo.csv'
            f = open(path, 'r', encoding="latin_1")
            copy_sql = """
                        COPY sexo from stdin WITH CSV HEADER
                        DELIMITER as ','
                        """
            cursor.copy_expert(sql=copy_sql, file=f)
            f.close()
            path = 'res/catalogos/tipopaciente.csv'
            f = open(path, 'r', encoding="latin_1")
            copy_sql = """
                        COPY tipopaciente from stdin WITH CSV HEADER
                        DELIMITER as ','
                        """
            cursor.copy_expert(sql=copy_sql, file=f)
            f.close()
            path = 'res/catalogos/sector.csv'
            f = open(path, 'r', encoding="latin_1")
            copy_sql = """
                                    COPY sector from stdin WITH CSV HEADER
                                    DELIMITER as ','
                                    """
            cursor.copy_expert(sql=copy_sql, file=f)
            f.close()
            conn.commit()
            elapsed_t = time.process_time() -t
            log.log('files copied to db', elapsed_t)
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            elapsed_t = time.process_time() - t
            log.log("Error: %s" % error, elapsed_t)
            return 1