# gfsRaster
This repository constains a few scripts to download (descarga_GFS025.py), reproyect and convert in raster (genGeotiff.py), and get statistics from a shapefile (geotiffToBasins.py).

## Examples of use:
### Downloading data
This script downloads data from nomads, using the grib filter

```python
python descarga_GFS025.py --ini 2021022200 --out data/GEFS --model gefs --nhours 240
```
- gfs: downloads gfs model, upo to 16 days (384 hours)
- gefs: downloads average data, up to 240 that are aviable on nomads, 0.25
- gefs05: should download the 35 days (841 hours aprox) 0.5

### Downloading data
