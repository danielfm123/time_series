#!/bin/python3
from __init__ import *
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option("max_columns", 30)
from selenium import webdriver
from selenium.webdriver.common.by import By
import datetime
import re
import sys

#https://si3.bcentral.cl/IndicadoresSiete/secure/IndicadoresDiarios.aspx

def scrap(serie,anio):

    con = db_geekosas("dfischer")
    params = pd.read_sql("select id_serie, url from dfischer.dm_serie where serie = '{}'".format(serie),con)
    id_serie = params.id_serie[0]
    url = params.url[0]

    #scrapper
    options = webdriver.FirefoxOptions()
    options.headless = True
    driver = webdriver.Firefox(options=options)

    driver.get(url)
    elem_anio = driver.find_element(By.XPATH,"//select[@name='DrDwnFechas']/option[@value={}]".format(str(anio)))
    elem_anio.click()

    #scrap
    tabla_raw = []
    elem_filas = driver.find_elements(By.XPATH,"//tr[@class='GridNormal' or @class='GridAlternate']")
    for f in elem_filas:
        filas = f.find_elements(By.XPATH,"td")
        valores = [x.text.replace(".","").replace(",",'.') for x in filas]
        valores = valores[1:]
        tabla_raw.append(valores)

    driver.close()

    #carga BD
    dataset = []
    updated = datetime.datetime.now()
    for d in range(len(tabla_raw)):
        #print(d+1)
        fila = tabla_raw[d]
        for m in range(len(fila)):
            #print(m+1)
            try:
                valor = float(tabla_raw[d][m])
                fecha = datetime.date(anio,m+1,d+1)
                print(fecha.strftime("%Y-%m-%d") + ': ' + str(valor))
                row = {"id_serie":id_serie,"fecha":fecha,"valor":valor,"updated":updated}
                dataset.append(row)
                #set_value(id_serie,fecha,valor,con)
            except:
                pass

    dataset = pd.DataFrame(dataset)

    key = ['id_serie','fecha']
    tabla = "dfischer.valor_serie"
    upsert(con,tabla,dataset,key)
    con.close()

if __name__ == "__main__":
    #pagina
    if len(sys.argv[1:]) == 1:
            hoy = datetime.date.today()
            anio = hoy.year
    else:
        serie = sys.argv[1]
        anio = int(sys.argv[2])

    scrap(serie,anio)