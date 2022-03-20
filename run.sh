#!/bin/bash

# ejemplo de esperar hasta hoy a las 10
# sleep $(bc <<<s$(date -f - +'t=%s.%N;' <<<$'22:00 today\nnow')'st-t')

# un parametro con cli anasac
# fecha_proc=$(/opt/lib_anasac/anasac/cli_anasac.py proc param get fecha_proc)
# o sin dependencias
# fecha_proc=$(curl https://192.168.100.240:300/param/fecha_proc/ -k)

# ip = $(/opt/lib_anasac/anasac/cli_anasac.py api get db_atenea_server)

# Python
# De ser necesario Activar VENV
# source /opt/venv_name
# echo paso xxx
# python3 script.py $@

# deactivate

# R
echo Ventas_V2
Rscript run.R $@


