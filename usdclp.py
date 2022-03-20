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

def scrap(anio):
    options = webdriver.FirefoxOptions()
    options.headless = True
    driver = webdriver.Firefox(options=options)

    if anio > 2012:
        #scrapper
        url = "https://www.sii.cl/valores_y_fechas/dolar/dolar{}.htm".format(anio)
        driver.get(url)

        #parametros
        elem_mes = driver.find_element(By.XPATH,"//button[@data-id='sel_mes']")
        elem_mes.send_keys('todos')
        elem_todos = driver.find_element(By.XPATH,"//div[@class='dropdown-menu open']/ul/li[1]")
        elem_todos.click()

        #scrap
        tabla_raw = []
        elem_filas = driver.find_elements(By.XPATH,"//table[@id='table_export']/tbody/tr")
        for f in elem_filas:
            filas = f.find_elements(By.XPATH,"td")
            valores = [x.text.replace(".","").replace(",",'.') for x in filas]
            tabla_raw.append(valores)

        driver.close()


    else:
        url = "https://www.sii.cl/pagina/valores/dolar/dolar{}.htm".format(anio)
        driver.get(url)

        #scrap
        tabla_raw = []
        elem_filas = driver.find_elements(By.XPATH,"//table[@class='tabla']/tbody/tr")
        for f in elem_filas[:-1]:
            filas = f.find_elements(By.XPATH,"td")
            valores = [x.text.replace(",",'.') for x in filas]
            #if(anio <= 2003):
            #    valores = valores[1:]
            tabla_raw.append(valores)

        driver.close()



    #carga BD
    dataset = []
    id_serie = 6
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
    con = db_geekosas("dfischer")   
    upsert(con,"dfischer.valor_serie",dataset,['id_serie', 'fecha'])    
    con.close()

if __name__ == "__main__":
    #pagina
    if len(sys.argv[1:]) == 0:
            hoy = datetime.date.today()
            anio = hoy.year
    else:
        anio = int(sys.argv[1])

    scrap(anio)