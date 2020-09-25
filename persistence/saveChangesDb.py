from logger import Logger
import time
from persistence.extra_vars import getFecha
"""class with method to copy data to daily table on postgres database"""


class SaveDataChanges:

    def SaveData(self, conn):
        """receives connection variable and path of csv file, insterts row by row data to database
        function(conn[connector from postgres_connect class], path[path of csv file]) -> instert rows
        and change values before insertions
        """
        t = time.process_time()
        log = Logger()
        cursor = conn.cursor()
        fecha = getFecha(cursor)
        sqlQuery = "insert into cambios("\
        "select d2.id_registro,"\
           "CASE WHEN od.origen <> d2.origen THEN 1 ELSE 0 END                         as origen,"\
           "CASE WHEN od.sector <> d2.sector THEN 1 ELSE 0 END                         as sector,"\
           "CASE WHEN od.entidad_um <> d2.entidad_um THEN 1 ELSE 0 END                 as entidad_um,"\
           "CASE WHEN od.sexo <> d2.sexo THEN 1 ELSE 0 END                             as sexo,"\
           "CASE WHEN od.entidad_nac <> d2.entidad_nac THEN 1 ELSE 0 END               as entidad_nac,"\
           "CASE WHEN od.entidad_res <> d2.entidad_res THEN 1 ELSE 0 END               as entidad_res,"\
           "CASE WHEN od.municipio_res <> d2.municipio_res THEN 1 ELSE 0 END           as municipio_res,"\
           "CASE WHEN od.tipo_paciente <> d2.tipo_paciente THEN 1 ELSE 0 END           as tipo_paciente,"\
           "CASE WHEN od.fecha_ingreso <> d2.fecha_ingreso THEN 1 ELSE 0 END           as fecha_ingreso,"\
           "CASE WHEN od.fecha_sintomas <> d2.fecha_sintomas THEN 1 ELSE 0 END         as fecha_sintomas,"\
           "CASE WHEN od.fecha_def <> d2.fecha_def THEN 1 ELSE 0 END                   as fecha_def,"\
           "CASE WHEN od.intubado <> d2.intubado THEN 1 ELSE 0 END                     as intubado,"\
           "CASE WHEN od.neumonia <> d2.neumonia THEN 1 ELSE 0 END                     as neumonia,"\
           "CASE WHEN od.edad <> d2.edad THEN 1 ELSE 0 END                             as edad,"\
           "CASE WHEN od.nacionalidad <> d2.nacionalidad THEN 1 ELSE 0 END             as nacionalidad,"\
           "CASE WHEN od.embarazo <> d2.embarazo THEN 1 ELSE 0 END                     as embarazo,"\
           "CASE WHEN od.habla_lengua_indig <> d2.habla_lengua_indig THEN 1 ELSE 0 END as habla_lengua,"\
           "CASE WHEN od.diabetes <> d2.diabetes THEN 1 ELSE 0 END                     as diabetes,"\
           "CASE WHEN od.epoc <> d2.epoc THEN 1 ELSE 0 END                             as epoc,"\
           "CASE WHEN od.asma <> d2.asma THEN 1 ELSE 0 END                             as asma,"\
           "CASE WHEN od.inmusupr <> d2.inmusupr THEN 1 ELSE 0 END                     as inmunospr,"\
           "CASE WHEN od.hipertension <> d2.hipertension THEN 1 ELSE 0 END             as hipertension,"\
           "CASE WHEN od.otra_com <> d2.otra_com THEN 1 ELSE 0 END                     as otra_com,"\
           "CASE WHEN od.cardiovascular <> d2.cardiovascular THEN 1 ELSE 0 END         as cardiovascular,"\
           "CASE WHEN od.obesidad <> d2.obesidad THEN 1 ELSE 0 END                     as obesidad,"\
           "CASE WHEN od.renal_cronica <> d2.renal_cronica THEN 1 ELSE 0 END           as renal_cronica,"\
           "CASE WHEN od.tabaquismo <> d2.tabaquismo THEN 1 ELSE 0 END                 as tabaquismo,"\
           "CASE WHEN od.otro_caso <> d2.otro_caso THEN 1 ELSE 0 END                   as otro_Caso,"\
           "CASE WHEN od.resultado <> d2.resultado THEN 1 ELSE 0 END                   as resultado,"\
           "CASE WHEN od.migrante <> d2.migrante THEN 1 ELSE 0 END                     as migrante,"\
           "CASE WHEN od.pais_nacionalidad <> d2.pais_nacionalidad THEN 1 ELSE 0 END   as pais_nacionalidad,"\
           "CASE WHEN od.pais_origen <> d2.pais_origen THEN 1 ELSE 0 END               as pais_origen,"\
           "CASE WHEN od.uci <> d2.uci THEN 1 ELSE 0 END                               as uci "\
    "from new_data d2 "\
             "right join old_data od on d2.id_registro = od.id_registro "\
    "where od.id_registro = d2.id_registro "\
      "and (d2.origen, d2.sector, d2.entidad_um, d2.sexo, d2.entidad_nac, d2.entidad_res, d2.municipio_res,"\
           "d2.tipo_paciente, d2.fecha_ingreso, d2.fecha_sintomas,"\
           "d2.fecha_def, d2.intubado, d2.neumonia, d2.edad, d2.nacionalidad, d2.embarazo, d2.habla_lengua_indig,"\
           "d2.diabetes, d2.epoc, d2.asma, d2.inmusupr, d2.hipertension, d2.otra_com, d2.cardiovascular, d2.obesidad,"\
           "d2.renal_cronica, d2.tabaquismo, d2.otro_caso, d2.resultado, d2.migrante, d2.pais_nacionalidad,"\
           "d2.pais_origen, d2.uci)"\
        "<> (od.origen, od.sector, od.entidad_um, od.sexo, od.entidad_nac, od.entidad_res, od.municipio_res,"\
            "od.tipo_paciente, od.fecha_ingreso, od.fecha_sintomas,"\
            "od.fecha_def, od.intubado, od.neumonia, od.edad, od.nacionalidad, od.embarazo, od.habla_lengua_indig,"\
            "od.diabetes, od.epoc, od.asma, od.inmusupr, od.hipertension, od.otra_com, od.cardiovascular, od.obesidad,"\
            "od.renal_cronica, od.tabaquismo, od.otro_caso, od.resultado, od.migrante, od.pais_nacionalidad,"\
            "od.pais_origen, od.uci)"\
")"
        cursor.execute(sqlQuery)
        conn.commit()
        sqlQuery = "UPDATE cambios set fecha = %s where fecha is null"
        cursor.execute(sqlQuery, [fecha[0]])
        conn.commit()
        cursor.close()

        elapsed_t = time.process_time() - t
        log.log('Datos de cambios copiados a tabla', elapsed_t)