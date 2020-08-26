import config
import os
from dao_covid import DaoCovid
from logger import Logger

def cls(): print("\n" * 50)


val = True
while val:
    print("Bienvenido a Sistema Covid-Uabc\n")
    print("Seleccione una opcion:\n")
    print("1.- Descarga de Base de datos\n")
    print("2.- Salir\n")
    val = input()
    if val == '1':
        DaoCovid()

    else:
        val = False

    cls()