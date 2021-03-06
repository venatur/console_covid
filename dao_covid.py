from persistence.unzip_res import Unzip
from persistence.postgres_connect import Connection
from config import Config
from persistence.download_url import DownloadUrl
from persistence.copy_csv2psql import CopyCsv
import os
from extras.create_dbs import CreateDb
from persistence.create_cat import CreateCat
from persistence.create_dir import CreateDir
from persistence.toRegistros import ToRegistros
from persistence.saveChangesDb import SaveDataChanges
from persistence.save_nuevos import SaveNuevos
from persistence.deleteFromDB import DeleteFromDB
from persistence.yesterdayData import YesterdayData
from persistence.saveCountData import SaveCountNuevos


class DaoCovid(Connection):

    def __init__(self):
        self.objc = Connection()
        self.url = Config.URL
        self.path = Config.ZIP_FILE
        self.path_r = Config.UPLOAD_FOLDER
        self.extension = Config.EXTENSION
        self.cat_url = Config.CATALOGOS
        self.hist_path = Config.HISTORY

        #self.create_dir()
        #self.searching_ext()
        #self.download_resource()
        #self.path_csv = self.unzip_resource()
        #self.create_db()
        #self.copy_data_todb()
        #self.copy_data_cat()
        #self.compareOldNew()
        #self.saveChanges()
    def download_resource(self):
        obj_down = DownloadUrl()
        obj_down.download(self.url, self.path)

    def unzip_resource(self):
        obj_unzip = Unzip()
        path_csv = obj_unzip.unizip_file(self.path, self.path_r)
        return path_csv

    def searching_ext(self):
        result = os.listdir(self.path_r)
        for item in result:
            if item.endswith('.{}'.format(self.extension)):
                path_url = os.path.join(self.path_r, item)
                os.remove(path_url)

    def create_db(self):
        conn = self.objc.connect()
        obj_create = CreateDb()
        obj_create.create_table(conn)

    def copy_data_todb(self, path_csv):
        conn = self.objc.connect()
        obj_copy = CopyCsv()
        obj_copy.copy_from_file(conn, path_csv)

    def copy_data_cat(self):
        conn = self.objc.connect()
        obj_copy = CreateCat()
        obj_copy.crea_catalogo(conn)

    def create_dir(self):
        obj_create = CreateDir()
        obj_create.create_dir(self.path_r)

    def compareOldNew(self):
        conn = self.objc.connect()
        obj_regs = ToRegistros()
        obj_regs.copyToRegistros(conn)

    def saveChanges(self):
        conn = self.objc.connect()
        obj_regs = SaveDataChanges()
        obj_regs.SaveData(conn)

    def saveNuevos(self):
        conn = self.objc.connect()
        obj_regs = SaveNuevos()
        obj_regs.SaveData(conn)

    def deleteDb(self):
        conn = self.objc.connect()
        obj_regs = DeleteFromDB(conn)
        obj_regs.deleteData()

    def saveYesterday(self):
        conn = self.objc.connect()
        obj_regs = YesterdayData()
        obj_regs.SaveData(conn)

    def saveCountNuevos(self):
        conn = self.objc.connect()
        obj_regs = SaveCountNuevos()
        obj_regs.SaveData(conn)

