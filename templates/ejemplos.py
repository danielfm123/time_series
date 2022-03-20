#!/usr/bin/python3
################ NO BORRAR #######################
import os
import sys

while not any([x in  [".git","files","Dockerfile",".gitignore",".dockerignore","project.Rproj"] for x in os.listdir()]):
    os.chdir("..")
print("Working Directory: " + os.getcwd())

if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())
    
if not '/opt/lib_anasac' in sys.path:
    sys.path.append('/opt/lib_anasac')

import anasac
################ NO BORRAR #######################
  
# Ejemplos
# argumentos
args = sys.argv[1:]

# parametros
anasac.set_param("ejemplo","hola")
anasac.get_param("ejemplo")

#escribir un log
anasac.write_log("nombre_proceso",exit_code = 0,mensaje = "hola mundo!")

# esperar a que termine un proceso
anasac.wait_completed(proc='setup_inicio_dia')

# Ejemplo SQL
import pandas as pd
con_atenea = anasac.db_atenea("SANDBOX")

data  = {'Brand': ['Honda Civic','Toyota Corolla','Ford Focus','Audi A4'],
        'Price': [22000,25000,27000,35000]}
data = pd.DataFrame(data)
# insertar
data.to_sql("borrar",con_atenea,schema="dbo",if_exists="append",index=False)
# bulk insert
atenea_bulk_insert_dataframe(data,"SANDBOX.dbo.borrar")
pd.read_sql('select * from SANDBOX.dbo.borrar',con_atenea)
pd.read_sql("""select * 
              from SANDBOX.dbo.borrar 
              where Price > {}""".format(26000),con_atenea)
con_atenea.execute('drop table dbo.borrar')
# si no funciona la query usar:
anasac.sql_execute_query(con,"drop table dbo.borrar")
con_atenea.close()

#escribir en blob
blob_string = anasac.key_vault.get_value('azure_analytics300_string')
blob_service_client = BlobServiceClient.from_connection_string(blob_string)
container_name = "backup-carga-archivos"
blob_file = params["proc"]+'/'+ str(anasac.datetime.datetime.now()) + " " + archivo

blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_file)
print("\nUploading to Azure Storage as blob:\n\t" + file_path)
with open(file_path, "rb") as data:
    blob_client.upload_blob(data)

# loop que imprime a medida que avanza (necesario en rstudio)
import time
import sys
for i in range(5):
   print(str(i))
   sys.stdout.flush() # imprime
   time.sleep(1.5) # sleep for 1.5 sec
   
   
# captura de excepcion
try:
  a = 1+1
  print(a)
  raise ValueError("Mensaje de Error")
  
  print("esto no se va a ejecutar :(")
  
  anasac.write_log("nombre proceso")
except Exception as e:
  print(e)
  anasac.write_log("nombre proceso",exit_code = -1, mensaje = str(e))



