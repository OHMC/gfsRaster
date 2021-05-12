#!/bin/bash


usage() { echo "$(basename "$0") [-h] [-f AAAAMMDDHH] [-d x] [-p x] [-r x] [-v x] [-t x] [-g x] [-m x]

Programa para correr operativo el WRF, con opción de ensamble y postprocesamiento

opciones:
    -h  Mostrar esta ayuda
    -f  Fijar una fecha particular en el formato AAAAMMDDHH (default: fecha actual)
    -d  Descargar GFS, 1 para descargar 0 para no descargar (default: 1)
    " 1>&2; exit 1; }

while getopts ':hf:' option; do
  case "$option" in
    h) usage
        ;;
    f)  export Y=`date -d "${OPTARG:0:-2} ${OPTARG:8:10}" +%Y`
            export M=`date -d "${OPTARG:0:-2} ${OPTARG:8:10}" +%m`
            export D=`date -d "${OPTARG:0:-2} ${OPTARG:8:10}" +%d`
            export H=`date -d "${OPTARG:0:-2} ${OPTARG:8:10}" +%H`
            ;;
    :) printf "Falta argumento para -%s\n" "$OPTARG" >&2
       usage
       ;;
   \?) printf "No existe esta opcion: -%s\n" "$OPTARG" >&2
       usage
       ;;
  esac
done
shift $((OPTIND-1))

source env.sh

source ~/.bashrc

mkdir -p ${RUN_DIR}
mkdir -p ${RUN_DIR}/GFS
mkdir -p ${RUN_DIR}/geotiff
mkdir -p ${RUN_DIR}/csv
mkdir -p ${RUN_DIR}/log


~/.pyenv/versions/3.8.0/bin/ray start --head --port=6380 --num-cpus=4

time ~/.pyenv/versions/3.8.0/bin/python3 descarga_GFS025.py --ini ${RUNDATE} --out ${RUN_DIR}/GFS/ --model gfs --nhours ${NHOURS} > ${RUN_DIR}/log/descarga.log 2>&1

time ~/.pyenv/versions/3.8.0/bin/python3 genGeotiff.py --path "${RUN_DIR}/GFS/GFS*.grib2" > ${RUN_DIR}/log/gen.log 2>&1

time ~/.pyenv/versions/3.8.0/bin/python3 gfsProducts.py --path "${RUN_DIR}/geotiff" --target "zonas" --shapefile shapefiles/Zonas_Cobertura_Cuidades.shp  > ${RUN_DIR}/log/geo.log 2>&1

time ~/.pyenv/versions/3.8.0/bin/python3 ingestor.py --path ${RUN_DIR}/csv/GFS_zonas_T2P.csv >> ${RUN_DIR}/log/ingestor.log 2>&1

time ~/.pyenv/versions/3.8.0/bin/python3 ingestor.py --path ${RUN_DIR}/csv/GFS_zonas_wspd.csv >> ${RUN_DIR}/log/ingestor.log 2>&1

time ~/.pyenv/versions/3.8.0/bin/python3 ingestor.py --path ${RUN_DIR}/csv/GFS_zonas_wdir.csv >> ${RUN_DIR}/log/ingestor.log 2>&1

~/.pyenv/versions/3.8.0/bin/ray stop

