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
  param[["proc"]] = paste(param,collapse = '.')
  lock(param[["proc"]])  
  
  # una vez identificado el proceso, se comienza el codigo cargando librerias necesarias
  # library()
  
  data <- sqlGetQuery(db_atenea,'select * from xxx')
  # otras conexiones
  # db_hortus
  # db_gleba
  
  ########manipular data
  
  
  
  #######guardar
  
  # Borrar data
  sqlExecute(db_atenea,"delete from @proc@ where ...", param)
  
  # guardar Data forma 1 (lenta, pero sirve para todas las bases y crea la tabla)
  table_id <- Id(catalog = param[['db']], schema = param[['schema']], table = param[['table']])
  sqlWriteTable(db_atenea,data, table_id)
  # forma 2 solo para atenea (muy rapido, no crea la tabla)
  atenea_bulk_insert_dataframe(data,param[['proc']],threads = 8)
  
  # log de ejecucion correctaa
  unlock(param[["proc"]])  
  write_log(param[['proc']])
  
},error=function(e) {
  
  #log de error, guarda error
  unlock(param[["proc"]])  
  write_log(param[['proc']],exit_code = -1,mensaje = paste(e$message,collapse = " "))
  print(e$message)}
)