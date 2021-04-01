#!/bin/bash

DAY=$(date -d "$D" '+%d')
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

RUNDATE="${YEAR}${MONTH}${DAY}00"


ray start --head --port=6380 --num-cpus=4

time python descarga_GFS025.py --ini ${RUNDATE} --out data/GFS/ --model gfs --nhours 168 

time python genGeotiff.py --path "data/GFS/GFS*.grib2"

time python geotiffToBasins.py --path "geotiff/${YEAR}_${MONTH}/${DAY}_00/GFS_*_T*.tiff" --target "zonas" --shapefile shapefiles/Zonas_Cobertura.shp


ray stop
