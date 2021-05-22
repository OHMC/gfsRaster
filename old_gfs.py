import rasterio
import glob
import pathlib
import argparse
import ray
from pathlib import Path
from affine import Affine
from datetime import datetime, timedelta
from osgeo import osr, gdal
from config.constants import KM_PER_DEGREE, EXTENT

ray.init(address='auto', _redis_password='5241590000000000')


def gen_list_dates(start: str, end: str):
    starts = datetime.strptime(start, '%Y%m%d%H')
    ends = datetime.strptime(end, '%Y%m%d%H')
    step = timedelta(days=1)

    result = []
    while starts < ends:
        result.append(starts.strftime('%Y_%m/%d_%H'))
        starts += step

    return result


def getPpnBand(grib):
    """ De un Dataset de gdal devuelve la banda donde se encuentra
    la variable APCP
    """
    dictVar = {}
    for band in range(1, grib.RasterCount + 1):
        var = grib.GetRasterBand(band)
        if var.GetMetadata()['GRIB_ELEMENT'] in ("UGRD"):
            if var.GetMetadata()['GRIB_SHORT_NAME'] in ("10-HTGL"):
                dictVar[int(band)] = "U10" 
        if var.GetMetadata()['GRIB_ELEMENT'] in ("VGRD"):
            if var.GetMetadata()['GRIB_SHORT_NAME'] in ("10-HTGL"):
                dictVar[int(band)] = "V10"
    return dictVar


def getInfo(filename: str):
    """Retorna la parametrizacion y el timestamp a partir del
    nombre del archivo wrfout
    """
    member = None
    filename = filename.split('/')[-1]
    model, timestamp = filename.split('_', 1)
    if model == 'GEFS':
        member, timestamp = timestamp.split('_', 1)
    daterun, ends = timestamp.split('+', 1)
    date = datetime.strptime(daterun, "%Y%m%d%H") + timedelta(hours=int(ends.split('.')[0]))

    return model, date, member


def getList(path: str):
    files = glob.glob(path, recursive=True)

    filtred_list = []
    for file in files:
        if (int(file[-9:-6]) >= 6) & (int(file[-9:-6]) <= 33):
            filtred_list.append(file)
    return filtred_list


def getGeoT(extent, nlines, ncols):
    # Compute resolution based on data dimension
    resx = (extent[2] - extent[0]) / ncols
    resy = (extent[3] - extent[1]) / nlines
    return [extent[0], resx, 0, extent[3], 0, -resy]


@ray.remote
def transformGrib(filename: str):
    print(f"Processing: {filename}")
    model, date, member = getInfo(filename)
    # print(f"Processing {filename}")
    # Read the GRIB file
    grib = gdal.Open(filename)
    if not grib:
        print("Dataset not compatible with GDAL")
        return

    dictVar = getPpnBand(grib)
    # print(f"Band {bandNumber} of type {type(bandNumber)}")

    if not dictVar:
        print("The dataset doesnt cointain ANY value")
        return

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

    # DESTINATION DATASET
    # Lat/lon WSG84 Spatial Reference System
    targetPrj = osr.SpatialReference()
    targetPrj.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')

    sizex = int((EXTENT[2] - EXTENT[0]) * KM_PER_DEGREE)
    sizey = int((EXTENT[3] - EXTENT[1]) * KM_PER_DEGREE)

    memDriver = gdal.GetDriverByName('MEM')

    # Create grid
    grid = memDriver.Create('grid', sizex, sizey, 1, gdal.GDT_Float64)

    # Setup projection and geo-transformation
    grid.SetProjection(targetPrj.ExportToWkt())
    grid.SetGeoTransform(getGeoT(EXTENT, grid.RasterYSize, grid.RasterXSize))

    for band in dictVar:
        # Read an specific band: Total Precipation
        bandGrid = grib.GetRasterBand(band)
        # write band in Dataset
        origin.GetRasterBand(1).WriteRaster(0, 0,
                                            grib.RasterXSize,
                                            grib.RasterYSize,
                                            grib.GetRasterBand(band).ReadRaster())

        # Build filename
        seconds = int(bandGrid.GetMetadata()['GRIB_VALID_TIME'][2:12])
        # seconds_run = int(bandGrid.GetMetadata()['GRIB_REF_TIME'][2:12])
        datetime_base = datetime(1970, 1, 1, 0, 0)
        # datetime_run = datetime_base + timedelta(0, seconds_run)
        # run = datetime_run.strftime('%H')
        datetimetiff = datetime_base + timedelta(0, seconds)
        tiffname = f"{model}_{member}_{dictVar[band]}_{datetimetiff.strftime('%Y-%m-%dZ%H:%M')}.tiff"
        path = (f"/home/datos/geotiff/{dictVar[band]}")
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        pathfile = f"{path}/{tiffname}"

        # do not process if the file exist
        if Path(pathfile).is_file():
            print("File exist")
            continue

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

        print(f"Saving {pathfile}")
        # WRITE GIFF
        nw_ds = rasterio.open(pathfile, 'w', driver='GTiff',
                              height=grid.RasterYSize,
                              width=grid.RasterXSize,
                              count=1,
                              dtype=gdal.GetDataTypeName(gdal.GDT_Float64).lower(),
                              crs=grid.GetProjection(),
                              transform=transform)
        nw_ds.write(array1, 1)
        nw_ds.close()

        bandGrid = None

    grib = None


def main():
    parser = argparse.ArgumentParser(
                description='genGeotiff.py --path=data/GEFS/*.grib2',
                epilog="Convert  all grib2 files stored in path folder \
                        to a raster in geoTiff format")

    parser.add_argument("--start", type=str, dest="start",
                        help="start date", required=True)
    parser.add_argument("--stop", type=str, dest="stop",
                        help="end date", required=True)
    args = parser.parse_args()

    # define options
    parser.print_help()

    dates = gen_list_dates(args.start, args.stop)

    # 'data/GFS/*.grib2'
    for date_path in dates:
        base_path = '/home/datos/wrfdatos/GFS025'
        filelist = getList(f"{base_path}/{date_path}/*")
        filelist.sort()

        it = ray.util.iter.from_items(filelist, num_shards=4)
        proc = [transformGrib.remote(filename) for filename in it.gather_async()]
        ray.get(proc)


if __name__ == "__main__":
    main()
