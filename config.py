import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class Config(object):

    LOGGER = 'log.txt'
    UPLOAD_FOLDER = r'res/'
    CATALOGOS = r'res/catalogos/'
    DB_FOLDER = 'database'
    EXTENSION = 'csv'
    URL = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
    #URL = "http://epidemiologia.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
    ZIP_FILE = "res/covid.zip"
    PARAMS_DIC = {
        "host": "localhost",
        "database": "covid",
        "user": "postgres",
        "password": "12345"

    }