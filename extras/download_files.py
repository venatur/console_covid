import requests
from persistence.create_dir import CreateDir
from persistence.download_url import DownloadUrl
obj = CreateDir()
path = r'../res/history/'
#obj.create_dir(r'./res/history/')
ZIP_FILE = "../res/history/"
url_octubre = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/10/datos_abiertos_covid19_01.10.2020.zip"
url_septiembre = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/09/datos_abiertos_covid19_01.09.2020.zip"
url_agosto = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/08/datos_abiertos_covid19_01.08.2020.zip"
url_julio = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/07/datos_abiertos_covid19_01.07.2020.zip"
url_junio = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/06/datos_abiertos_covid19_01.06.2020.zip"
url_mayo = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/05/datos_abiertos_covid19_01.05.2020.zip"
url_abril = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/04/datos_abiertos_covid19_01.04.2020.zip"
url_octubre = list(url_octubre)
url_septiembre = list(url_septiembre)
url_agosto = list(url_agosto)
url_julio = list(url_julio)
url_junio = list(url_junio)
url_mayo = list(url_mayo)
url_abril = list(url_abril)




def deserialize(url):
    lista_f = []
    for item in range(1, 32):
        if item <10:
            url[-13] = str(item)
        else:
            lista = list(str(item))
            url[-14] = lista[0]
            url[-13] = lista[1]
        listToStr = ''.join([str(elem) for elem in url])
        lista_f.append(listToStr)
    return lista_f


def descarga(url, save_path):
    obj = DownloadUrl()
    for item in url:
        obj.download(item, save_path+item[-23:])

octubre = deserialize(url_octubre)
septiembre = deserialize(url_septiembre)
agosto = deserialize(url_agosto)
julio = deserialize(url_julio)
junio = deserialize(url_junio)
mayo = deserialize(url_mayo)
abril = deserialize(url_abril)
descarga(octubre, ZIP_FILE)
descarga(septiembre, ZIP_FILE)


