import rasterio
import glob
import argparse
from affine import Affine
from datetime import datetime, timedelta
from osgeo import osr, gdal

extent = [-75, -40, -58, -25]
# Define KM_PER_DEGREE
KM_PER_DEGREE = 111.32

GEFS_REGEX = r"wrfout_(?P<param>[A-Z])_[a-z0-9]{3,4}_(?P<timestamp>\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2})"


def getInfo(filename: str):
    """Retorna la parametrizacion y el timestamp a partir del
    nombre del archivo wrfout"""
    m = re.match(WRFOUT_REGEX, wrfout)
    if not m:
        logger.critical("No se pudo obtener la configuracion, proporcione"
                        "una desde los parametros de ejecición.")
        raise ValueError
    m_dict = m.groupdict()
    param = m_dict.get('param')
    timestamp = datetime.datetime.strptime(m_dict.get('timestamp'),
                                           '%Y-%m-%d_%H:%M:%S')
    return param, timestamp


def getList(path: str):
    return glob.glob(path, recursive=True)


def getGeoT(extent, nlines, ncols):
    # Compute resolution based on data dimension
    resx = (extent[2] - extent[0]) / ncols
    resy = (extent[3] - extent[1]) / nlines
    return [extent[0], resx, 0, extent[3], 0, -resy]


def transformGrib(filename: str, model: str):
    # Select model
    if model == 'gfs':
        bandNumber = 145
    elif model == 'gefs':
        bandNumber = 7

    # Read the GRIB file
    grib = gdal.Open(filename)

    # Read an specific band: Total Precipation
    band = grib.GetRasterBand(bandNumber)

    # ORIGIN DATASET
    # Create grid
    originDriver = gdal.GetDriverByName('MEM')
    origin = originDriver.Create('grid',
                                 grib.RasterXSize,
                                 grib.RasterYSize,
                                 1, gdal.GDT_Float64)

    # Setup projection and geo-transformation
    origin.SetProjection(grib.GetProjection())
    origin.SetGeoTransform(grib.GetGeoTransform())

    # write band in Dataset
    origin.GetRasterBand(1).WriteRaster(0, 0,
                                        grib.RasterXSize,
                                        grib.RasterYSize,
                                        grib.GetRasterBand(bandNumber).ReadRaster())

    # DESTINATION DATASET
    # Lat/lon WSG84 Spatial Reference System
    targetPrj = osr.SpatialReference()
    targetPrj.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')

    sizex = int((extent[2] - extent[0]) * KM_PER_DEGREE)
    sizey = int((extent[3] - extent[1]) * KM_PER_DEGREE)

    memDriver = gdal.GetDriverByName('MEM')

    # Create grid
    grid = memDriver.Create('grid', sizex, sizey, 1, gdal.GDT_Float64)

    # Setup projection and geo-transformation
    grid.SetProjection(targetPrj.ExportToWkt())
    grid.SetGeoTransform(getGeoT(extent, grid.RasterYSize, grid.RasterXSize))

    # Perform the projection/resampling

    gdal.ReprojectImage(
        origin,
        grid,
        grib.GetProjection(),
        targetPrj.ExportToWkt(),
        gdal.GRA_NearestNeighbour,
        options=['NUM_THREADS=ALL_CPUS']
                       )

    # Read grid data
    array1 = grid.ReadAsArray()

    # Get transform in Affine format
    geotransform = grid.GetGeoTransform()
    transform = Affine.from_gdal(*geotransform)

    # Build filename
    seconds = int(band.GetMetadata()['GRIB_VALID_TIME'][2:12])
    datetimetiff = datetime(1970, 1, 1, 0, 0) + timedelta(0, seconds)
    tiffname = f"GEFS_02_PPN_{datetimetiff.strftime('%Y-%m-%dZ%H:%M')}.tiff"
    path = f"geotiff/{tiffname}"

    # WRITE GIFF
    nw_ds = rasterio.open(path, 'w', driver='GTiff',
                          height=grid.RasterYSize,
                          width=grid.RasterXSize,
                          count=1,
                          dtype=gdal.GetDataTypeName(gdal.GDT_Float64).lower(),
                          crs=grid.GetProjection(),
                          transform=transform)
    nw_ds.write(array1, 1)
    nw_ds.close()

    grib = None


def main():
    parser = argparse.ArgumentParser(
                description='genGeotiff.py --path=data/GEFS/*.grib2',
                epilog="Convert  all grib2 files stored in path folder \
                        to a raster in geoTiff format")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with grib2", required=True)

    parser.add_argument("--model", type=str, dest="model",
                        help="if it's gfs or gefs", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    # 'data/GFS/*.grib2'
    filelist = getList(args.path)

    for filename in filelist:
        transformGrib(filename, args.model)


if __name__ == "__main__":
    main()
