import geopandas as gpd
import pandas as pd
import numpy as np
import ray
import glob
import argparse
import os
import re
from rasterstats import zonal_stats, point_query
from datetime import datetime

ray.init(address='auto', _redis_password='5241590000000000')


def filterByTarget(filelist: list, target: str):
    if target == "cuencas":
        match_dir = re.compile('.*GFS_None_ACPC.*')
    if target == "zonas":
        match_dir = re.compile('.*GFS_None_(T0|T2|VGRD|UGRD).*')
    if target == "sur":
        match_dir = re.compile('.*GFS_None_(V10|U10).*')

    return [s for s in filelist if match_dir.match(s)]


def getInfo(filename: str):
    """Retorna la parametrizacion y el timestamp a partir del
    nombre del archivo wrfout
    geotiff/GEFS_01_PPN_2021-02-23Z00:00.tiff"""
    pert = None
    filename = filename.split('/')[-1]
    model, timestamp = filename.split('_', 1)
    pert, timestamp = timestamp.split('_', 1)
    var, timestamp = timestamp    .split('_', 1)
    timestamp, extension = timestamp.split('.', 1)
    date = datetime.strptime(timestamp, "%Y-%m-%dZ%H:%M")

    return model, date, pert, var


def getList(regex: str):
    regex = regex + '/*'
    return glob.glob(regex, recursive=True)


def integrate_shapes(filename: str, shapefile: str,
                     target: str, cuencas_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    This functions opens a geotiff with desired data, converts to a raster,
    integrate the data into polygons and returns a GeoDataFrame object.

    Parameters:
        cuencas_shp: Path to shapefile
    Returns:
        cuencas_gdf_ppn (GeoDataFrame): a geodataframe with cuerncas and ppn
    """

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


def integrate_shapes_sur(filename: str, shapefile: str) -> gpd.GeoDataFrame:
    """
    This functions opens a geotiff with desired data, converts to a raster,
    integrate the data into polygons and returns a GeoDataFrame object.

    Parameters:
        cuencas_shp: Path to shapefile
    Returns:
        cuencas_gdf_ppn (GeoDataFrame): a geodataframe with cuerncas and ppn
    """

    cuencas_gdf: gpd.GeoDataFrame = gpd.read_file(shapefile, encoding='utf-8')
    df_zs = pd.DataFrame(point_query(shapefile, filename))

    cuencas_gdf_ppn = pd.concat([cuencas_gdf, df_zs], axis=1)

    COLUM_REPLACE = {0: 'data'}

    cuencas_gdf_ppn = cuencas_gdf_ppn.rename(columns=COLUM_REPLACE)

    return cuencas_gdf_ppn[['NAME', 'geometry', 'data']]


@ray.remote
def selectBasin(filename, shapefile, target, cuencas_gdf):
    model, date, pert, var = getInfo(filename)
    rioii = pd.DataFrame()

    cuencas_gdf = integrate_shapes(filename, shapefile, target, cuencas_gdf)
    cuencas_gdf = cuencas_gdf.loc[cuencas_gdf["subcuenca"].str.contains('Tercero')]
    cuencas_gdf = cuencas_gdf[['subcuenca', 'mean']]
    cuencas_gdf['date'] = datetime.strptime(filename[-21:-5], "%Y-%m-%dZ%H:%M")
    rioii = rioii.append(cuencas_gdf, ignore_index=True)
    run_dir = os.getenv('RUN_DIR')
    filename = f"{run_dir}/csv/{model}_{pert}_{var}_all.csv"
    print(f"Saving in {filename}")
    rioii.to_csv(filename, mode='a', header=False, encoding='utf-8')


@ray.remote
def zonalEpec(filename: str, shapefile: str, target: str, cuencas_gdf: gpd.GeoDataFrame):
    model, date, pert, var = getInfo(filename)

    zonas = pd.DataFrame()

    zonas_gdf = integrate_shapes(filename, shapefile, target, cuencas_gdf)
    zonas_gdf = zonas_gdf[['zona', 'mean']]
    zonas_gdf['date'] = datetime.strptime(filename[-21:-5], "%Y-%m-%dZ%H:%M")
    zonas = zonas.append(zonas_gdf, ignore_index=True)
    run_dir = os.getenv('RUN_DIR')
    filename = f"{run_dir}/csv/{model}_{target}_{var}_all.csv"
    print(f"Saving in {filename}")
    zonas.to_csv(filename, mode='a', header=False, encoding='utf-8')


@ray.remote
def zonal_sur(filename: str, shapefile: str, cuencas_gdf: gpd.GeoDataFrame):
    model, date, pert, var = getInfo(filename)

    zonas = pd.DataFrame()

    zonas_gdf = integrate_shapes_sur(filename, shapefile)
    zonas_gdf = zonas_gdf[['NAME', 'data']]
    zonas_gdf['date'] = date
    zonas = zonas.append(zonas_gdf, ignore_index=True)
    filename = f"csv/{var}.csv"
    print(f"Saving in {filename}")
    zonas.to_csv(filename, mode='a', header=False, encoding='utf-8')


def accumDiario(target: str):
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
        run_dir = os.getenv('RUN_DIR')
        dialy.to_csv(f"{run_dir}/csv/{name}_{target}_day.{exten}", encoding='utf-8')


def getT2product(dfT2, dfTSK, awsname, param):
    """ Obtiene un pronostico de temperatura a partir de las variables
    T2 y TSK
    """
    mask = dfTSK['mean'].values - dfT2['mean'].values
    mask = mask > 0
    maskinverted = np.invert(mask)

    fieldname = f"T2P_{awsname}_{'param'}"
    dfT2 = dfT2.rename(columns={'mean': fieldname})
    dfTSK = dfTSK.rename(columns={'mean': fieldname})

    append = dfT2[mask].append(dfTSK[maskinverted], sort=True)
    append = append[['name', 'date', f'{fieldname}']]
    append.sort_index(inplace=True)

    return append


def genT2P(target: str):
    # Open generated CSV
    run_dir = os.getenv('RUN_DIR')
    data_T0_file = f'{run_dir}/csv/GFS_zonas_T0_all.csv'
    data_T2_file = f'{run_dir}/csv/GFS_zonas_T2_all.csv'
    data_T0 = pd.read_csv(data_T0_file, header=None,  encoding='utf-8')
    data_T2 = pd.read_csv(data_T2_file, header=None,  encoding='utf-8')

    data_T0["name"] = data_T0[1]
    data_T0["mean"] = data_T0[2]
    data_T0["date"] = pd.to_datetime(data_T0[3])
    data_T0 = data_T0[['name', 'mean', 'date']]
    data_T2["name"] = data_T2[1]
    data_T2["mean"] = data_T2[2]
    data_T2["date"] = pd.to_datetime(data_T2[3])
    data_T2 = data_T2[['name', 'mean', 'date']]

    # Get unique values of zones
    zonas = data_T0.name.unique()
    # select by zone
    for zona in zonas:
        zona_T0 = data_T0.loc[data_T0['name'] == zona]
        zona_T2 = data_T2.loc[data_T2['name'] == zona]

        zona_T0 = zona_T0.sort_values(by='date')
        zona_T2 = zona_T2.sort_values(by='date')

        data = getT2product(zona_T2, zona_T0, zona, 'T2P')
        file_out = f'{run_dir}/csv/GFS_zonas_T2P.csv'
        data.to_csv(file_out, mode='a', header=None, encoding='utf-8')


def genWind():
    # Open generated CSV
    run_dir = os.getenv('RUN_DIR')
    file_spd = f'{run_dir}/csv/GFS_zonas_wspd.csv'
    file_wdir = f'{run_dir}/csv/GFS_zonas_wdir.csv'
    data_U_file = f'{run_dir}/csv/GFS_zonas_UGRD_all.csv'
    data_V_file = f'{run_dir}/csv/GFS_zonas_VGRD_all.csv'
    data_U = pd.read_csv(data_U_file, header=None)
    data_V = pd.read_csv(data_V_file, header=None)

    data_U["name"] = data_U[1]
    data_U["mean"] = data_U[2]
    data_U["date"] = pd.to_datetime(data_U[3])
    data_U = data_U[['name', 'mean', 'date']]
    data_V["name"] = data_V[1]
    data_V["mean"] = data_V[2]
    data_V["date"] = pd.to_datetime(data_V[3])
    data_V = data_V[['name', 'mean', 'date']]

    # Get unique values of zones
    zonas = data_U.name.unique()

    for zona in zonas:
        zona_U = data_U.loc[data_U['name'] == zona]
        zona_V = data_V.loc[data_V['name'] == zona]

        WDIR = (270-np.rad2deg(np.arctan2(zona_V['mean'], zona_U['mean']))) % 360
        WSPD = np.sqrt(np.square(zona_V['mean'])+np.square(zona_U['mean']))

        zona_wspd = zona_V[['name', 'date']]
        zona_wdir = zona_V[['name', 'date']]
        zona_wdir.loc[:, 'wdir'] = WDIR.values
        zona_wspd.loc[:, 'wspd'] = WSPD.values

        zona_wdir.sort_index(inplace=True)
        zona_wspd.sort_index(inplace=True)

        zona_wspd.to_csv(file_spd, mode='a', header=None, encoding='utf-8')
        zona_wdir.to_csv(file_wdir, mode='a', header=None, encoding='utf-8')


def getBasisns(filelist: list, shapefile: str, target: str):

    filelist.sort()
    it = ray.util.iter.from_items(filelist, num_shards=4)
    if target == "cuencas":
        cuencas_gdf: gpd.GeoDataFrame = gpd.read_file(shapefile)
        proc = [selectBasin.remote(filename, shapefile, target, cuencas_gdf) for filename in it.gather_async()]
        ray.get(proc)
        accumDiario(target)
    elif target == "zonas":
        cuencas_gdf: gpd.GeoDataFrame = gpd.read_file(shapefile)
        proc = [zonalEpec.remote(filename, shapefile, target, cuencas_gdf) for filename in it.gather_async()]
        ray.get(proc)
        genT2P(target)
        genWind()
    elif target == "sur":
        cuencas_gdf: gpd.GeoDataFrame = gpd.read_file(shapefile)
        proc = [zonal_sur.remote(filename, shapefile, cuencas_gdf) for filename in it.gather_async()]
        ray.get(proc)

    #    print(f"Processing {filename}")
    #    processGiff.remote(filename, shapefile)


def geotiffToBasisns(regex: str, shapefile: str, target: str):
    filelist = getList(regex)
    if not filelist:
        print("ERROR: No geotiff files matched")
        return
    filteredList = filterByTarget(filelist, target)
    if not filteredList:
        print(f"ERROR: No bariables for {target}")
        return
    getBasisns(filteredList, shapefile, target)


def main():
    # regex = "geotiff/GEFS_01_PPN_*.tiff"
    # shapefile = "../../wrf-cuenca/src/shapefiles/cuencas_hidro_new.shp"

    parser = argparse.ArgumentParser(
                description=('gfsProducts.py --path=geotiff/GEFS*.tiff '
                             '--shapefile=shapefiles/basisn.shp'),
                epilog="Extract info from rasters")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with gtiff files", required=True)

    parser.add_argument("--target", type=str, dest="target",
                        help="zonas or basins or sur", required=True)

    parser.add_argument("--shapefile", type=str, dest="shapefile",
                        help="if it's gfs or gefs", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    geotiffToBasisns(args.path, args.shapefile, args.target)


if __name__ == "__main__":
    main()
