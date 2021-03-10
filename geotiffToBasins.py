import geopandas as  gpd
import pandas as pd
import ray
import glob
import argparse
from rasterstats import zonal_stats
from datetime import datetime, timedelta

COLUM_REPLACE = {'Subcuenca': 'subcuenca', 'Cuenca': 'cuenca'}

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

    return model, date, pert


def getList(regex: str):
    return glob.glob(regex, recursive=True)


def integrate_basins(basins_fp: str, shapefile: str) -> gpd.GeoDataFrame:
    """
    This functions opens a geotiff with ppn data, converts to a raster,
    integrate the ppn into cuencas and returns a GeoDataFrame object.

    Parameters:
        cuencas_shp: Path to shapefile
    Returns:
        cuencas_gdf_ppn (GeoDataFrame): a geodataframe with cuerncas and ppn
    """

    cuencas_gdf: gpd.GeoDataFrame = gpd.read_file(shapefile)
    df_zs = pd.DataFrame(zonal_stats(shapefile, basins_fp, all_touched=True))

    cuencas_gdf_ppn = pd.concat([cuencas_gdf, df_zs], axis=1).dropna(subset=['mean'])

    cuencas_gdf_ppn = cuencas_gdf_ppn.rename(columns=COLUM_REPLACE)

    return cuencas_gdf_ppn[['subcuenca', 'cuenca', 'geometry', 'count',
                            'max', 'min', 'mean']]


def getBasisns(filelist: list, shapefile: str):
   
    pert = None
    it = ray.util.iter.from_items(filelist, num_shards=4)

    for filename in it.gather_async():
        print(f"Processing {filename}")
        model, date, pert = getInfo(filename)
        rioii = pd.DataFrame()

        cuencas_gdf = integrate_basins(filename, shapefile)
        cuencas_gdf = cuencas_gdf.loc[cuencas_gdf["subcuenca"].str.contains('Tercero')]
        cuencas_gdf = cuencas_gdf[['subcuenca', 'mean']]
        cuencas_gdf['date'] = datetime.strptime(filename[-21:-5], "%Y-%m-%dZ%H:%M")
        rioii = rioii.append(cuencas_gdf, ignore_index=True)

        rioii.to_csv(f"data/csv/{model}_{pert}_ppm_all.csv",
                     mode='a',
                     header=False)
                # dialy = rioii.resample('D', on='date').sum()
                # dialy.to_csv(f"data/csv/{model}_{lastpert}_ppn_diario.csv",
                #              mode='a',
                #              header=False)
    
    filelistcsv = glob.glob(f"data/csv/GEFS_*.csv")

    for filecsv in filelistcsv:
        GEFS = pd.read_csv(filecsv, header=None) 
        GEFS["mean"] = GEFS[2]
        GEFS["date"] = pd.to_datetime(GEFS[3])
        GEFS = GEFS[["mean", "date"]]
        GEFS = GEFS.iloc[1:] # the first data is trash
        GEFS.set_index('date')
        dialy = GEFS.resample('D', on="date").sum()
    
        filename = filecsv.split('/')[-1]
        name, exten = filename.split('.')
        dialy.to_csv(f"data/csv/{name}_day.{exten}")


def geotiffToBasisns(regex: str, shapefile: str):
    filelist = getList(regex)
    if not filelist:
        print("ERROR: No geotiff files matched")
        return
    getBasisns(filelist, shapefile)


def main():
    # regex = "geotiff/GEFS_01_PPN_*.tiff"
    # shapefile = "../../wrf-cuenca/src/shapefiles/cuencas_hidro_new.shp"


    parser = argparse.ArgumentParser(
                description='geotiffToBasins.py --path=geotiff/GEFS*.tiff --shapefile=shapefiles/basisn.shp',
                epilog="Convert  all grib2 files stored in path folder \
                        to a raster in geoTiff format")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with geoti", required=True)

    parser.add_argument("--shapefile", type=str, dest="shapefile",
                        help="if it's gfs or gefs", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    geotiffToBasisns(args.path, args.shapefile)

if __name__ == "__main__":
    main()
