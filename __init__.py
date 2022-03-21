import pyodbc
import sqlalchemy
import os
import datetime
import random
import pandas as pd

#### Conexiones a SQL ####

def get_random_word(wordLen):
  word = ''
  for i in range(wordLen):
      word += random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
  return word

# geekosas
def db_geekosas(db="dfischer"):
  con_parameters = {
    "user": os.environ["mysql_user"],
    "pass": os.environ["mysql_pass"],
    "server": "18.221.120.35",
    "db":db,
    'port':'3306'
    }
  # con_parameters = {x:urllib.parse.quote_plus(con_parameters[x]) for x in con_parameters}
  conn_str = "mysql+pymysql://{user}:{pass}@{server}:{port}/{db}?charset=utf8mb4"
  # conn_str = "Driver={driver};Server={server},{port};UID={user};PWD={pass};Database={db}"
  conn_str = conn_str.format(**con_parameters)
  con = sqlalchemy.create_engine(conn_str,echo=False).connect()#.execution_options(autocommit=True)
  # con = pyodbc.connect(conn_str)
  return con

def upsert(con,tabla,dataset,key):
  try:
    temp_table = "upsert_" + get_random_word(30)
    con.execute("create table {} select * from {} limit 0".format(temp_table,tabla))
    dataset.to_sql(temp_table,con,if_exists='append',index=False)

    if(isinstance(key, str)):
      key = key.split(',')
    
    where = ['t.{0} = tmp.{0}'.format(k) for k in key]
    where = ' and '.join(where)
    
    values = pd.read_sql("select * from {} limit 0".format(tabla),con).columns
    values =  [v for v in values if v not in key]
    update = ['t.{0} = tmp.{0}'.format(v) for v in values]
    update = ", ".join(update)
    

    #update
    sql_execute_query(con,"""
                        update {tabla} t
                        inner join {temp_table} tmp
                        on {where}
                        set {update}""".format(tabla = tabla,temp_table = temp_table , where = where, update = update))
    #insert
    sql_execute_query(con,"""insert into {tabla} 
                              select tmp.* from {temp_table} tmp
                              left join {tabla} t
                              on {where}
                              where t.{key} is null""".format(tabla = tabla, temp_table = temp_table, where = where, key = key[0]))

    con.execute("""drop table {}""".format(temp_table))
    #sql_execute_query(con,"""drop table {}""".format(temp_table))
    return True
  except:
    con.execute("""drop table {}""".format(temp_table))
    return False

def set_value(id_serie,fecha, valor,con):
  hora = datetime.datetime.now()
  hora = hora.strftime("%Y-%m-%d %H:%M:%S")
  str_fecha = fecha.strftime("%Y-%m-%d")

  query_existencia = "select count(*) from dfischer.valor_serie where id_serie = {} and fecha = '{}'".format(id_serie,str_fecha)
  if con.execute(query_existencia).__next__()[0] == 0:
    query_create = "INSERT into dfischer.valor_serie(id_serie,fecha) values({},'{}')".format(id_serie,str_fecha)
    con.execute(query_create)

  query_update = "update dfischer.valor_serie set valor = '{}', updated = '{}' where id_serie = {} and fecha = '{}'".format(valor,hora,id_serie,fecha)
  con.execute(query_update)

  return True

def sql_execute_query(con,query, retry_count = 0, max_retry = 0):
  from sqlalchemy.orm import sessionmaker
  Session = sessionmaker(bind=con)
  session = Session()
  try:
    result = session.execute(query)
    session.commit()
  except Exception as e:
    if (retry_count < max_retry):
      retry_count = retry_count + 1
      print("retry {}".format(retry_count))
      sql_execute_query(con,query, retry_count = retry_count, max_retry = max_retry)
    else:
      raise Exception(str(e))
  session.close()

