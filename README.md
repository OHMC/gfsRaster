# gfsRaster
This repository constains a few scripts to download (descarga_GFS025.py), reproyect and convert in raster (genGeotiff.py), and get statistics from a shapefile (geotiffToBasins.py).
Paralelized using Ray Framework

## Examples of use:
### Downloading data
This script downloads data from nomads, using the grib filter, variables downoaded inside the file

```python
python descarga_GFS025.py --ini 2021022200 --out data/GEFS --model gefs --nhours 240
```
- gfs: downloads gfs model, upo to 16 days (384 hours)
- gefs: downloads average data, up to 240 that are aviable on nomads, 0.25
- gefs05: should download the 35 days (841 hours aprox) 0.5

### Converting to raster
This script converts the grib2 files to rasters

```python
time python genGeotiff.py --path "data/GFS/*.grib2"
```

- path: regex of files to convert

### Rasterize the raster
To get statcal information from a geotiff, based on a shapefile

```python
time python geotiffToBasins.py --path "geotiff/GFS_None_PPN_2021-03-*.tiff" --shapefile shapefiles/cuencas_hidro_new.shp --target zonas
```

## TODO:
- Specify the variable to rasterize  (genGeotiff.py)
- Implement logging


