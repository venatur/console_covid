from dao_covid import DaoCovid
import schedule
import time
from logger import Logger
from datetime import datetime, date
from persistence.deleteFromDB import DeleteFromDB


def job():
    today = str(date.today())
    current_time = time.process_time()
    obj_log.log('saved at ' + today, current_time)
    objD.delete_new_data()
    obj.searching_ext()
    obj.download_resource()
    path_csv = obj.unzip_resource()
    # newdata
    obj.copy_data_todb(obj.path_r+path_csv)
    # registros
    obj.compareOldNew()
    # cambios
    obj.saveChanges()
    # nuevos
    obj.saveNuevos()
    # oldata
    objD.delete_old_data()
    # oldata
    obj.saveYesterday()
    # nuevos registros
    obj.saveCountNuevos()


obj_log = Logger()
obj = DaoCovid()
objD = DeleteFromDB()
schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

