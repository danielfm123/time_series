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

def load_file(archivo):
  try:
    
    params = {
      'db' : 'SANDBOX',
      'schema' : 'dbo',
      'table' : 'borrar'
    }
    params["proc"] = '.'.join(params.values())
    
    import pandas as pd
    # pip3 install azure-storage-blob --user
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
    
    file_path = './files/' + archivo
    data = pd.read_excel(file_path)
    #preparar la tabla
    
    
    
    #limpar antes de cargar
    con = anasac.db_atenea(params['db'])
    anasac.sql_execute_query(con,'truncate table {}'.format(params["proc"]))
    
    # ingresar los datos,hay 2 formas
    # pandas
    data.to_sql(params["proc"],con_atenea,schema=params['schema'],if_exists="append",index=False)
    # bulk insert
    anasac.atenea_bulk_insert_dataframe(data,params["proc"])

    # queda pendiente respandar en blob
    blob_string = anasac.key_vault.get_value('azure_analytics300_string')
    blob_service_client = BlobServiceClient.from_connection_string(blob_string)
    container_name = "backup-carga-archivos"
    blob_file = params["proc"]+'/'+ str(anasac.datetime.datetime.now()) + " " + archivo

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_file)
    print("\nUploading to Azure Storage as blob:\n\t" + file_path)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data)
    
    #limpiar y log
    os.remove(file_path)
    con.close()
    anasac.write_log(tabla,exit_code = 0,mensaje = blob_file)
    
  except Exception as e:
    print(e)
    anasac.write_log(tabla,exit_code = -1, mensaje = str(e))



if __name__ == '__main__':
  args = sys.argv[1:]
  archivo = args[0]
  # archivo = "sap.sta.faglflext.csv"
  # archivo = "SAP.sta.MARA.csv"
  load_file(archivo)
