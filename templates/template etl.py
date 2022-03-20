#!/bin/python3
################ NO BORRAR #######################
import os
import sys

while not any([x in  [".git","files","project.Rproj"] for x in os.listdir()]):
    os.chdir("..")
# print("Working Directory: " + os.getcwd())

if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())
    
if not '/opt/lib_anasac' in sys.path:
    sys.path.append('/opt/lib_anasac')

import anasac
################ NO BORRAR #######################

try:
  # en general... proceso identificado por tabla de destino
  params = {
    'db' : 'SANDBOX',
    'schema' : 'dbo',
    'table' : 'tabla_borrar'
  }
  params["proc"] = '.'.join(params.values())
  anasac.lock(params["proc"])
  
  # una vez identificado el proceso, se comienza el codigo cargando librerias necesarias
  import pandas as pd

  # conectarse al sql orgigen
  con_origen = anasac.db_hortus()
  data = pd.read_sql('select * from .....')

  #preparar la data....
  
  # conexion a base destino
  con = anasac.db_atenea(params['db'])
  #limpar antes de cargar
  anasac.sql_execute_query(con,'truncate table {}'.format(params["proc"]))
  
  # ingresar los datos,hay 2 formas
  # pandas
  data.to_sql(params["proc"],con_atenea,schema=params['schema'],if_exists="append",index=False)
  # bulk insert
  anasac.atenea_bulk_insert_dataframe(data,params["proc"])

  #limpiar y log
  con_origen.close()
  con.close()
  
  anasac.unlock(params["proc"])
  anasac.write_log(params["proc"])
  
except Exception as e:
  print(e)
  anasac.unlock(params["proc"])
  anasac.write_log(params["proc"],exit_code = -1, mensaje = str(e))

