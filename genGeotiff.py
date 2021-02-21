import rasterio
import glob
from affine import Affine
from datetime import datetime, timedelta
from osgeo import osr, gdal

extent = [-75, -40, -58, -25]
# Define KM_PER_DEGREE
KM_PER_DEGREE = 111.32


def getList():
    return glob.glob('data/GFS/*.grib2', recursive=True)


def getGeoT(extent, nlines, ncols):
    # Compute resolution based on data dimension
    resx = (extent[2] - extent[0]) / ncols
    resy = (extent[3] - extent[1]) / nlines
    return [extent[0], resx, 0, extent[3], 0, -resy]


def transformGrib(filename: str):
    # Read the GRIB file
    grib = gdal.Open(filename)

    # Read an specific band: Total Precipation
    band = grib.GetRasterBand(145)

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
                                        grib.GetRasterBand(145).ReadRaster())

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
    tiffname = f"GFS_PPN_{datetimetiff.strftime('%Y-%m-%dZ%H:%M')}.tiff"
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
    filelist = getList()

    for filename in filelist:
        transformGrib(filename)


if __name__ == "__main__":
    main()
