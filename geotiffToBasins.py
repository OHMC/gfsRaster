import geopandas as gpd
import pandas as pd
import ray
import glob
import argparse
from rasterstats import zonal_stats
from datetime import datetime, timedelta

ray.init(address='localhost:6380', _redis_password='5241590000000000')


def getInfo(filename: str):
    """Retorna la parametrizacion y el timestamp a partir del
    nombre del archivo wrfout
    geotiff/GEFS_01_PPN_2021-02-23Z00:00.tiff"""
    pert = None
    filename = filename.split('/')[-1]
    model, timestamp = filename.split('_', 1)
    pert, timestamp = timestamp.split('_', 1)
    var, timestamp = timestamp.split('_', 1)
    timestamp, extension = timestamp.split('.', 1)
    date = datetime.strptime(timestamp, "%Y-%m-%dZ%H:%M")

    return model, date, pert, var


def getList(regex: str):
    return glob.glob(regex, recursive=True)


def integrate_shapes(filename: str, shapefile: str,
                     target: str) -> gpd.GeoDataFrame:
    """
    This functions opens a geotiff with desired data, converts to a raster,
    integrate the data into polygons and returns a GeoDataFrame object.

    Parameters:
        cuencas_shp: Path to shapefile
    Returns:
        cuencas_gdf_ppn (GeoDataFrame): a geodataframe with cuerncas and ppn
    """

    cuencas_gdf: gpd.GeoDataFrame = gpd.read_file(shapefile)
    df_zs = pd.DataFrame(zonal_stats(shapefile, filename, all_touched=True))

    cuencas_gdf_ppn = pd.concat([cuencas_gdf,
                                 df_zs], axis=1).dropna(subset=['mean'])
    if target == "cuencas":
        COLUM_REPLACE = {'Subcuenca': 'subcuenca', 'Cuenca': 'cuenca'}
        cuencas_gdf_ppn = cuencas_gdf_ppn.rename(columns=COLUM_REPLACE)
        return cuencas_gdf_ppn[['subcuenca', 'cuenca', 'geometry', 'count',
                                'max', 'min', 'mean']]
    elif target == "zonas":
        COLUM_REPLACE = {'Name': 'zona'}
        cuencas_gdf_ppn = cuencas_gdf_ppn.rename(columns=COLUM_REPLACE)
        return cuencas_gdf_ppn[['zona', 'geometry', 'mean']]

    return None


@ray.remote
def selectBasin(filename, shapefile, target):
    model, date, pert, var = getInfo(filename)
    rioii = pd.DataFrame()

    cuencas_gdf = integrate_shapes(filename, shapefile, target)
    cuencas_gdf = cuencas_gdf.loc[cuencas_gdf["subcuenca"].str.contains('Tercero')]
    cuencas_gdf = cuencas_gdf[['subcuenca', 'mean']]
    cuencas_gdf['date'] = datetime.strptime(filename[-21:-5], "%Y-%m-%dZ%H:%M")
    rioii = rioii.append(cuencas_gdf, ignore_index=True)
    filename = f"data/csv/{model}_{pert}_{var}_all.csv"
    print(f"Saving in {filename}")
    rioii.to_csv(filename, mode='a', header=False)


@ray.remote
def zonalEpec(filename: str, shapefile: str, target: str):
    model, date, pert, var = getInfo(filename)

    zonas = pd.DataFrame()

    zonas_gdf = integrate_shapes(filename, shapefile, target)
    zonas_gdf = zonas_gdf[['zona', 'mean']]
    zonas_gdf['date'] = datetime.strptime(filename[-21:-5], "%Y-%m-%dZ%H:%M")
    zonas = zonas.append(zonas_gdf, ignore_index=True)
    filename = f"data/csv/{model}_{target}_{var}_all.csv"
    print(f"Saving in {filename}")
    zonas.to_csv(filename, mode='a', header=False)


def getBasisns(filelist: list, shapefile: str, target: str):

    filelist.sort()
    it = ray.util.iter.from_items(filelist, num_shards=4)
    if target == "cuencas":
        proc = [selectBasin.remote(filename, shapefile, target) for filename in it.gather_async()]
    elif target == "zonas":
        proc = [zonalEpec.remote(filename, shapefile, target) for filename in it.gather_async()]
    ray.get(proc)
    #    print(f"Processing {filename}")
    #    processGiff.remote(filename, shapefile)

    filelistcsv = glob.glob(f"data/csv/*{target}*.csv")

    for filecsv in filelistcsv:
        GEFS = pd.read_csv(filecsv, header=None)
        GEFS["mean"] = GEFS[2]
        GEFS["date"] = pd.to_datetime(GEFS[3])
        GEFS = GEFS[["mean", "date"]]
        GEFS = GEFS.iloc[1:]  # the first data is trash
        GEFS.set_index('date')
        dialy = GEFS.resample('D', on="date").sum()

        filename = filecsv.split('/')[-1]
        name, exten = filename.split('.')
        dialy.to_csv(f"data/csv/{name}_{target}_day.{exten}")


def geotiffToBasisns(regex: str, shapefile: str, target: str):
    filelist = getList(regex)
    if not filelist:
        print("ERROR: No geotiff files matched")
        return
    getBasisns(filelist, shapefile, target)


def main():
    # regex = "geotiff/GEFS_01_PPN_*.tiff"
    # shapefile = "../../wrf-cuenca/src/shapefiles/cuencas_hidro_new.shp"

    parser = argparse.ArgumentParser(
                description=('geotiffToBasins.py --path=geotiff/GEFS*.tiff '
                             '--shapefile=shapefiles/basisn.shp'),
                epilog="Convert  all grib2 files stored in path folder \
                        to a raster in geoTiff format")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with geoti", required=True)

    parser.add_argument("--target", type=str, dest="target",
                        help="zonas or basins", required=True)

    parser.add_argument("--shapefile", type=str, dest="shapefile",
                        help="if it's gfs or gefs", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    geotiffToBasisns(args.path, args.shapefile, args.target)


if __name__ == "__main__":
    main()
