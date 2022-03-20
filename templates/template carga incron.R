################ NO BORRAR #######################
source("/opt/lib_anasac/anasac/init.R")
################ NO BORRAR #######################


tryCatch({
  # en general... proceso identificado por tabla de destino
  param = c(
    db = 'SANDBOX',
    schema = 'dbo',
    table = 'tabla_borrar'
  )
  param["proc"] = paste(param,collapse = '.')
  
  # system('./setup_rclone.sh',intern = F)
  library(AzureStor)
  # library(readxl)
  
  args = commandArgs(trailingOnly=TRUE)
  archivo = args[1]
  # archivo + "ejemplo.xlsx"
  print(paste("archivo:", archivo))
  
  file_path = paste0("files/",archivo)
  print(paste("full path:", file_path))
  
  #La hoja correcta de ABO es DIARIO, debe comenzar a leer desde la linea 1 
  
  data <- read_excel(file_path)
 
  ########manipular data
  
  
  
  #######guardar
  
  #Atenea
  sqlExecute(db_atenea,"delete from @proc@ where mes_carga = '@mes@'", param)
  
  table_id <- Id(catalog = param[['db']], schema = param[['schema']], table = param[['table']])
  sqlWriteTable(db_atenea,data, table_id)
  
  
  ######### backups en blob
  blob_key = key_vault$get_value("azure_analytics300_key")
  blob_endpoint = key_vault$get_value("azure_analytics300_endpoint")
  blob_endp <- blob_endpoint(blob_endpoint,key=blob_key)
  cont <- blob_container(blob_endp, "backup-carga-archivos")
  blob_name = paste0(param[['proc']],"/",Sys.time(), " ",archivo)
  storage_multiupload(cont, archivo, blob_name)
  
  
  file.remove(file_path)
  write_log(param[['proc']], mensaje = archivo, blob_name)
  
},error=function(e) {
  write_log(param[['proc']],exit_code = -1,mensaje = paste(e$message,collapse = " "))
  print(e$message)}
)