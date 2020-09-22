from persistence.unzip_res import Unzip
import os


def decompress():
    obj = Unzip()
    path_r = '../res/history/'
    extension = 'zip'

    result = os.listdir(path_r)
    lista = []
    abr = []
    may = []
    jun = []
    jul = []
    ago = []
    sep = []
    listado = []
    for item in result:
        if item.endswith('.{}'.format(extension)):
            path_url = os.path.join(path_r, item)
            lista.append(path_url)
    lista.sort()
    for item in range(len(lista)):
        if lista[item][-10] == '4':
            abr.append(lista[item])
        elif lista[item][-10] == '5':
            may.append(lista[item])
        elif lista[item][-10] == '6':
            jun.append(lista[item])
        elif lista[item][-10] == '7':
            jul.append(lista[item])
        elif lista[item][-10] == '8':
            ago.append(lista[item])
        elif lista[item][-10] == '9':
            sep.append(lista[item])

    listado = abr+may+jun+jul+ago+sep
    [obj.unizip_file(listado[item], path_r) for item in range(len(listado))]


decompress()
