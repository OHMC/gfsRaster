import requests
import argparse
import pandas as pd
from datetime import datetime
from config.constants import gfs_info, base_url, headers


def buildList(gfs_t2p: pd.DataFrame, aws_zones: list):
    for aws in aws_zones:
        temp = gfs_t2p.loc[gfs_t2p['zona'] == aws['nombre']]
        temp = temp.sort_values('date')
        uid = aws['id']
        # AAAAMMDDTHHMMSSZ
        registers_list = []
        i = 0
        for line in temp.iterrows():
            dict = {'fecha': datetime.strftime(line[1]['date'], '%Y%m%dT%H%M%SZ'),
                    'gfs_mean_region_temperature': {'data': line[1]['T2P'], 'forecast': i, 'info': gfs_info}
                    }
            i = i + 3
            registers_list.append(dict)

        # post vía apirest
        json_ = {'id': uid, 'lista_registros': registers_list,
                 'usa_claves': True, 'replace_existing': True}

        response = requests.post(base_url+'datos/', headers=headers, json=json_)
        if response.ok:
            print(response.json())
        else:
            print(response.content)


def getT2P(path: str):
    gfs_t2p = pd.read_csv(path, header=None, encoding='utf-8')
    gfs_t2p['T2P'] = gfs_t2p[1]
    gfs_t2p['date'] = pd.to_datetime(gfs_t2p[2])
    gfs_t2p['zona'] = gfs_t2p[3]
    gfs_t2p = gfs_t2p[['date', 'T2P', 'zona']]

    return gfs_t2p


def getAWS_Zonal():
    # get full list
    url = base_url + 'estaciones/'
    response = requests.get(url, headers=headers).json()
    aws_list = response['aws_list']

    # filter zonal
    # find zonal points
    aws_zones = []
    for estacion in aws_list:
        if estacion['metadata']['red'] == 'EPEC':
            aws_zones.append(estacion)

    return aws_zones


def ingestor(path: str):
    aws_zones = getAWS_Zonal()
    gfs_t2p = getT2P(path)
    buildList(gfs_t2p, aws_zones)


def main():
    parser = argparse.ArgumentParser(
                description='ingestor.py --path=data/GEFS/*.grib2',
                epilog="Convert  all grib2 files stored in path folder \
                        to a raster in geoTiff format")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with csv to ingest", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    ingestor(args.path)
    # it = ray.util.iter.from_items(filelist, num_shards=4)
    # proc = [transformGrib.remote(filename) for filename in it.gather_async()]
    # ray.get(proc)


if __name__ == "__main__":
    main()
