import bco_central

for anio in range(1994,2023):
    print(anio)
    bco_central.scrap('CADCLP',anio)

anio = 2000
serie = 'BRLCLP'
bco_central.scrap(serie,anio)

#tabla = "dfischer.valor_serie"
#con = db_geekosas("dfischer")

