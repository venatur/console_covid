import psycopg2
from logger import Logger
import time
import csv
import sys


class CopyCsv:

    def copy_from_file(self, conn, path):
        t = time.process_time()
        log = Logger()
        cursor = conn.cursor()
        reader = csv.DictReader(open(path), doublequote=False)
        recordCount = 0
        try:
            for row in reader:
                f_ing = row['FECHA_INGRESO']
                f_sint = row['FECHA_SINTOMAS']
                f_def = row['FECHA_DEF']
                compar = '9999-99-99'
                if f_ing == '9999-99-99':
                    f_ing = None
                elif f_sint == compar:
                    f_sint = None
                elif f_def == compar:
                    f_def = None

                sqlInsert = \
                    "INSERT INTO daily (fecha_actualizacion, id_registro, origen, " \
                    "sector, entidad_um, sexo, entidad_nac, entidad_res, municipio_res," \
                    "tipo_paciente,fecha_ingreso, fecha_sintomas, fecha_def, intubado, neumonia, edad," \
                    "nacionalidad,embarazo, habla_lengua_indig, diabetes, epoc, asma, inmusupr, hipertension, otra_com," \
                    "cardiovascular, obesidad, renal_cronica, tabaquismo, otro_caso, resultado, migrante," \
                    "pais_nacionalidad, pais_origen, uci) VALUES (%s," \
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                cursor.execute(sqlInsert, [row['FECHA_ACTUALIZACION'], row['ID_REGISTRO'], row['ORIGEN'],
                                           row['SECTOR'],
                                           row['ENTIDAD_UM'],
                                           row['SEXO'],
                                           row['ENTIDAD_NAC'],
                                           row['ENTIDAD_RES'],
                                           row['MUNICIPIO_RES'],
                                           row['TIPO_PACIENTE'],
                                           f_ing,
                                           f_sint,
                                           f_def,
                                           row['INTUBADO'],
                                           row['NEUMONIA'],
                                           row['EDAD'],
                                           row['NACIONALIDAD'],
                                           row['EMBARAZO'],
                                           row['HABLA_LENGUA_INDIG'],
                                           row['DIABETES'],
                                           row['EPOC'],
                                           row['ASMA'],
                                           row['INMUSUPR'],
                                           row['HIPERTENSION'],
                                           row['OTRA_COM'],
                                           row['CARDIOVASCULAR'],
                                           row['OBESIDAD'],
                                           row['RENAL_CRONICA'],
                                           row['TABAQUISMO'],
                                           row['OTRO_CASO'],
                                           row['RESULTADO'],
                                           row['MIGRANTE'],
                                           row['PAIS_NACIONALIDAD'],
                                           row['PAIS_ORIGEN'],
                                           row['UCI']
                                           ])
                conn.commit()
        except csv.Error as e:
            sys.exit('file {}, line {}: {}'.format(path, reader.line_num, e))

        cursor.close()
        elapsed_t = time.process_time() - t
        log.log('Datos copiados a tabla', elapsed_t)

