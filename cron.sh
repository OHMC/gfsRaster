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




BASE='/opt/data/rasters/'

DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

OPDIR=${BASEDIR}${YEAR}_${MONTH}/

mkdir -p ${OPDIR}

