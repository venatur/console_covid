def getFecha(cursor):
    sqlQuery = "select fecha_actualizacion from new_data"
    cursor.execute(sqlQuery)
    fecha = cursor.fetchone()
    return fecha


def get_numero(cursor):
    sqlQuery = "select " \
               "(select count(*) from new_data) - " \
               "(select count(*) from old_data)"
    cursor.execute(sqlQuery)
    numero = cursor.fetchone()
    return numero
