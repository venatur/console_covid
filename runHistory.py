from dao_covid import DaoCovid
import os
from persistence.deleteFromDB import DeleteFromDB
from persistence.postgres_connect import Connection


def listar():
    result = os.listdir(obj.hist_path)

    print(obj.hist_path)
    for item in result:
        if item.endswith('.{}'.format(obj.extension)):
            path_url = os.path.join(obj.hist_path, item)
            print(path_url)

            objD = DeleteFromDB()
            objD.delete_new_data()
            #newdata
            obj.copy_data_todb(path_url)
            #registros
            obj.compareOldNew()
            #cambios
            obj.saveChanges()
            #nuevos
            obj.saveNuevos()
            #oldata
            objD.delete_old_data()
            #oldata
            obj.saveYesterday()
            #data historica
            obj.saveHistory()
    # nuevos registros
    obj.saveCountNuevos()

obj = DaoCovid()
listar()


