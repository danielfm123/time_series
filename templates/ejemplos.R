#!/usr/bin/Rscript
################ NO BORRAR #######################
source("/opt/lib_anasac/anasac/init.R")
################ NO BORRAR #######################


# Ejemplos
# argumentos
args = commandArgs(trailingOnly = TRUE)

# parametros
set_param("ejemplo","hola")
get_param("ejemplo")

# log de ejecución
write_log("nombre_proceso",exit_code = 0,mensaje = "hola mundo!")

# esperar a que termine proceso
wait_completed("setup_inicio_dia")

# SQL
data = iris
table_id <- Id(catalog = "SANDBOX", schema = "dbo", table = "borrar")
sqlWriteTable(db_atenea,data,table_id)
# bulk insert
atenea_bulk_insert_dataframe(data,"SANDBOX.dbo.borrar",threads = 8)

sqlGetQuery(db_atenea,'select * from SANDBOX.dbo.borrar')
sqlGetQuery(db_atenea,"select * 
                      from SANDBOX.dbo.borrar
                      where Species = '@especie@'",c(especie = 'setosa'))
sqlExecute(db_atenea,'drop table SANDBOX.dbo.borrar')

# leer y escribir en blob
library(AzureStor)
blob_key = key_vault$get_value("azure_analytics300_key")
blob_endpoint = key_vault$get_value("azure_analytics300_endpoint")
blob_endp <- blob_endpoint(blob_endpoint,key=blob_key)
cont <- blob_container(blob_endp, "test")
storage_multiupload(cont, "Readme.txt", "Readme.txt")

# captura de excepcion
tryCatch({
  a = 1+1
  print(a)
  stop("Mensaje de Error")
  
  print("esto no se va a ejecutar :(")
  
  write_log("nombre proceso")
},error = function(e){
  print(e$message)
  write_log("nombre proceso",exit_code = -1, mensaje = paste(e$mensaje,collapse = " "))
})
