import requests
import argparse
import pandas as pd
from datetime import datetime
from config.constants import gfs_info, base_url, token, variables


def get_config(filename: str):
    """ Retorna la parametrizacion, la variable y el RUN a partir del nombre del csv """

    filename = filename.split('/')[-1]
    model, temp = filename.split('_', 1)
    prod, temp = temp.split('_', 1)
    var, extension = temp.split('.', 1)

    return model, prod, var


def buildList(gfs_var: pd.DataFrame, aws_zones: list, var: str):

    headers = {'Authorization': 'Token ' + token}
    # Build List for ingestion
    for aws in aws_zones:
        temp = gfs_var.loc[gfs_var['zona'] == aws['nombre']]
        temp = temp.sort_values('date')
        uid = aws['id']

        # AAAAMMDDTHHMMSSZ
        registers_list = []
        i = 0
        for line in temp.iterrows():
            dict = {'fecha': datetime.strftime(line[1]['date'], '%Y%m%dT%H%M%SZ'),
                    variables[var]: {'data': line[2][var], 'forecast': i, 'info': gfs_info}
                    }
            i = i + 3
            registers_list.append(dict)

        # post vía apirest
        json_ = {'id': uid, 'lista_registros': registers_list,
                 'usa_claves': True, 'replace_existing': True}
        print(f"POST: {base_url}datos/")
        print(f"json: {json_}")
        response = requests.post(base_url+'datos/', headers=headers, json=json_)
        if response.ok:
            print(response.json())
        else:
            print(response.content)


def getCsvVar(path: str, var: str):
    # 'name', 'mean', 'date'
    # logger.info(f"Opening: {path}")
    gfs_var = pd.read_csv(path, header=None, encoding='utf-8')
    gfs_var[f'{var}'] = gfs_var[2]
    gfs_var['date'] = pd.to_datetime(gfs_var[2])
    gfs_var['zona'] = gfs_var[1]
    gfs_var = gfs_var[['date', f'{var}', 'zona']]

    return gfs_var


def getAWS_Zonal():
    # get full list
    headers = {'Authorization': 'Token ' + token}
    url = base_url + 'estaciones/'
    response = requests.get(url, headers=headers).json()
    aws_list = response['aws_list']

    # filter zonal
    # find zonal points
    aws_zones = []
    for estacion in aws_list:
        if estacion['metadata']['red'] == 'EPEC':
            aws_zones.append(estacion)
        if estacion['id'] == '300000000000000000367':
            aws_zones.append(estacion)

    return aws_zones


def ingestor(path: str):
    model, prod, var = get_config(path)
    # path: A csv with weather data
    aws_zones = getAWS_Zonal()
    gfs_var = getCsvVar(path, var)
    buildList(gfs_var, aws_zones, var)


def main():
    parser = argparse.ArgumentParser(
                description='ingestor.py --path=path/to/csv/file',
                epilog="Ingest csv")

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
