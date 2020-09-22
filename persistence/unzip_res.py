import zipfile
from logger import Logger
import time


class Unzip:

    def unizip_file(self, path, file):
        obj_log = Logger()
        t = time.process_time()

        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(file)
            elapsed_t = time.process_time() - t
            obj_log.log('Archivo descomprimido ' + file, elapsed_t)
            name = zip_ref.namelist()
            return name[0]


