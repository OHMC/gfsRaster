import geopandas as  gpd
import pandas as pd
import datetime
import glob
from rasterstats import zonal_stats

COLUM_REPLACE = {'Subcuenca': 'subcuenca', 'Cuenca': 'cuenca'}


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
    df_zs = pd.DataFrame(zonal_stats(shapefile, basins_fp))

    cuencas_gdf_ppn = pd.concat([cuencas_gdf, df_zs], axis=1).dropna(subset=['mean'])

    cuencas_gdf_ppn = cuencas_gdf_ppn.rename(columns=COLUM_REPLACE)

    return cuencas_gdf_ppn[['subcuenca', 'cuenca', 'geometry', 'count',
                            'max', 'min', 'mean']]


def getBasisns(filelist: list, shapefile: str):

    rioii = pd.DataFrame()

    for filename in filelist:
        print(f"Processing {filename}")
        cuencas_gdf = integrate_basins(filename, shapefile)
        cuencas_gdf = cuencas_gdf.loc[cuencas_gdf.index == 49]
        cuencas_gdf = cuencas_gdf[['subcuenca', 'mean']]
        cuencas_gdf['date'] = datetime.datetime.strptime(filename[-21:-5], "%Y-%m-%dZ%H:%M")
        rioii = rioii.append(cuencas_gdf, ignore_index=True)

    rioii.to_csv("GFS_ppm_all.csv")
    dialy = rioii.resample('D', on='date').sum()
    dialy.to_csv('GFS_ppn_diario.csv')

def geotiffToBasisns(regex: str, shapefile: str):
    filelist = getList(regex)
    getBasisns(filelist, shapefile)


def main():
    regex = "geotiff/GFS_PPN_*.tiff"
    shapefile = "../../wrf-cuenca/src/shapefiles/cuencas_hidro_new.shp"
    geotiffToBasisns(regex, shapefile)


if __name__ == "__main__":
    main()
