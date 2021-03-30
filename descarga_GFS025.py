# -*- coding: utf-8 -*-

# 0.25x0.25 horizontal resolution on the specified geographical domain
# and for the specified meteorological parameters only (slice, i.e.
# sub-area of global data)

# Attention: trailing spaces are mandatory in structured "if" or "for"

# for file-directory functions
import os
# for sleep command
import time
# for archiving (tar) of created files
import os.path
import requests
import datetime
import socket
import argparse

# CONFIGURATION OF GRIB FILTER AND DATE RANGE
# Domain (limited area) definition (geographical coordinates)
LON_W = "-96"
LON_E = "-15"
LAT_N = "-10"
LAT_S = "-75"

MODEL = ['gfs', 'gefs']

# Total forecast length (in hours) for which data are requested:
NHOUR = 35*24  # DIAS POR HORAS
# Interval in hours between two forecast times:
DHOUR = 3

# Count files to request
CANT_FILES_REQUESTED = NHOUR/DHOUR+1

COUNTMAX = 50
ICOUNTMAX = 100
S_SLEEP1 = 600
S_SLEEP2 = 60

# Definition of requested levels and parameters
# PAR_LIST=["TMAX"]
# PRES surface
# RH 2 m
# TMP 2m
# UGRD 10 m
# VGRD 10 m
# APCP 
PAR_LIST = ["APCP", "PRES", "RH",
             "TMP",  "UGRD", "VGRD"]

# ADDR
# TEMPLATE
# https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p50a.pl?file=gep01.t00z.pgrb2a.0p50.f024
# &lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_surface=on&var_APCP=on&var_PRES=on&var_RH=on&var_TMP=on
# &var_UGRD=on&var_VGRD=on&leftlon=-96&rightlon=-15&toplat=-10&bottomlat=-75&showurl=
# &dir=%2Fgefs.20210302%2F00%2Fatmos%2Fpgrb2ap5

# https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t00z.pgrb2.0p25.f003&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_surface=on&var_APCP=on&var_PRES=on&var_RH=on&var_TMP=on&var_UGRD=on&var_VGRD=on&leftlon=-96&rightlon=-15&toplat=-10&bottomlat=-75&dir=%2Fgfs.20210302%2F00

# https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t00z.pgrb2.0p25.f003&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_surface=on&var_APCP=on&var_PRES=on&var_RH=on&var_TMP=on&var_UGRD=on&var_VGRD=on&leftlon=-96&rightlon=-15&toplat=-10&bottomlat=-75&dir=%2Fgfs.20210302%2F00

BASE_ADDR = "https://nomads.ncep.noaa.gov/cgi-bin/"

BASE_FTP = "https://ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/"

DOMAIN_PARAMETERS = (f"&leftlon={LON_W}&rightlon={LON_E}"
                     f"&bottomlat={LAT_S}&toplat={LAT_N}")
LEVELS = "&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_surface=on"


def download_grb_file(url, outfile):
    """ Download grib files
    This function download an url of a Grib file and
    saves the file in outfile location and name

    Attributes:
        url (str): url of the grib gfile
        oufile (str): fullpath and name of the file

    Todo: chek if its an GRIB file
          activate logging

    """
    # cur_url = url.format(date=date)
    response = requests.get(url, stream=True)

    if not response.ok:
        print('Failed to download gfs data for date')
        return -1

    # logging.info('Start download DOMAIN_PARAMETERScontent(1024):
    with open(outfile, 'wb') as f:
        for block in response.iter_content(1024):
            f.write(block)

    # logging.info('Finished: {:%Y-%m-%d %H:%M}'.format(date))
    return 0


def get_list_gfs(inidate: str, model: str, nhours: str):
    inidate = int(inidate)      # CAST INIT date
    # inidate=int(os.environ['inidate'])

    # Date of forecast start (analysis)
    date = inidate//100
    # Instant (hour, UTC) of forecast start (analysis)

    # fci=int(sys.argv[1]) # reads input on the calling line
    # (example: "python get_GFS_grib2_slice.py 00")
    # fci=input('Type UTC time of analysis (00, 06, 12, 18) ')
    # # for request of manual input after launch
    fci = inidate-date*100

    # Defines connection timeoutcd
    socket.setdefaulttimeout(30)

    # Definiton of date (in required format)
    day = datetime.datetime.today()
    # tomorrow=day+datetime.timedelta(days=1)
    # yesterday=day+datetime.timedelta(days=-1)

    # If the download is made early in the morning,
    # the date is that of yesterday

    # if day.hour < 6 :
    #   day=yesterday

    # day="%4.4i%2.2i%2.2i" % (day.year,day.month,day.day)
    # # structure definition
    day = "%8.8i" % (date)
    fciA = "%2.2i" % fci

    print("Date and hour of GEFS forecast initial time: ", day, fci)

    # dir=%2Fgefs.20210218%2F00%2Fatmos%2Fpgrb2sp25
    # filter_gefs_atmos_0p50a.pl pgrb2ap5

    if model == 'gfs':
        PERL_FILTER = "filter_gfs_0p25.pl"
        dir_gfs_name = f"&dir=%2Fgfs.{day}%2F{fciA}%2Fatmos"
    elif model == 'gefs':
        PERL_FILTER = "filter_gefs_atmos_0p25s.pl"
        dir_gfs_name = f"&dir=%2Fgefs.{day}%2F{fciA}%2Fatmos%2Fpgrb2sp25"
    elif model == 'gefs05':
        PERL_FILTER = "filter_gefs_atmos_0p50a.pl"
        dir_gfs_name = f"&dir=%2Fgefs.{day}%2F{fciA}%2Fatmos%2Fpgrb2ap5"
                       

    # get list of var to download
    parameters = ""
    npar = len(PAR_LIST)
    for iparam in range(0, npar):
        parameters = parameters + f"&var_{PAR_LIST[iparam]}=on"

    # Full list of requested files
    list_remote_files = []
    list_files_local = []

    # for iter_num_file in range(0, int(CANT_FILES_REQUESTED)):
    path = f"{LEVELS}{parameters}{DOMAIN_PARAMETERS}{dir_gfs_name}"
    # ?file=geavg.t00z.pgrb2s.0p25.f000
    if model == 'gfs':
        hours = [f"{i:03}" for i in range(0, int(nhours)+1, 3)]
        for hfA2 in hours:
            file_name_base = f"?file=gfs.t{fciA}z.pgrb2.0p25.f{hfA2}"
            local_file = f"GFS_{day}{fciA}+{hfA2}.grib2"
            list_remote_files.append(f"{PERL_FILTER}{file_name_base}{path}")
            list_files_local.append(local_file)

    elif model == 'gefs':
        hours = [f"{i:03}" for i in range(0, int(nhours)+1, 3)]
        for hfA2 in hours:
            file_name_base = f"?file=geavg.t{fciA}z.pgrb2s.0p25.f{hfA2}"
            local_file = f"GEFS_{day}{fciA}+{hfA2}.0p25.grib2"
            list_remote_files.append(f"{PERL_FILTER}{file_name_base}{path}")
            list_files_local.append(local_file)

    elif model == "gefs05":
        # hours = np.arange(0, 241, 3).tolist()
        # hours.append(np.arange(246, 841, 6).tolist())
        hours = [f"{i:03}" for i in range(0, 241, 3)]
        hours = hours + [f"{i:03}" for i in range(246, 841, 6)]
        perturbations = [f"{i:02}" for i in range(1, 31, 1)]
        for hfA2 in hours:
            for pert in perturbations:
                file_name_base = f"?file=gep{pert}.t{fciA}z.pgrb2a.0p50.f{hfA2}"
                local_file = f"GEFS_{pert}_{day}{fciA}+{hfA2}.0p50.grib2"
                list_remote_files.append(f"{PERL_FILTER}{file_name_base}{path}")
                list_files_local.append(local_file)

    for key in range(len(list_remote_files)):
        print("************************************************************")
        print(f"Remote file Nº {key}: {list_remote_files[key]}")
        print("------------------------------------------------------------")
        print(f"Localfile file Nº {key}: {list_files_local[key]}")
        
    return list_remote_files, list_files_local


def download(output_dir, list_remote_files,
             list_files_local, server=BASE_ADDR):
    """
    Se descargan los datos con los siguientes parámetros::

        LON_W="-96"  # límite de longitud oeste de la grilla
        LON_E="-15"  # límite de longitud este de la grilla
        LAT_N="-10"  # límite de latitud norte de la grilla
        LAT_S="-75"  # límite de latitud sur de la grilla
        ADGRID="0.25"  # resolución de la grilla
        NHOUR=39  # cantidad de horas que en que se descargan datos
        DHOUR=03 # intervalo en horas en que se vuelve a descargar
        LEV_LIST=["all"] # niveles solicitaget_list_gfsdos
        PAR_LIST=["HGT","LAND","PRES","PRMSL","RH","SOILW","SPFH","TMP","UGRD",
        "VGRD","WEASD","TSOIL"]  # parámetros solicitados

    Parameters:
        inidate(str): date GFS files in the format YMDH with H 00, 06, 12 or 18
        output(str): path to the directory where GFS should be saved

    """

    # sys.exit(0)
    # Dowloading of requested files

    count_files_to_download = len(list_remote_files)
    print(f"Request in server: {server}")
    print(f"count of files to download: {count_files_to_download}")
    # extenar check, if this fail we go out
    not_downloaded_files = []

    # to check if all desired files were downloaded
    for ifile in range(0, count_files_to_download):

        remote_file = f"{server}{list_remote_files[ifile]}"
        local_file = (f"{output_dir}/{list_files_local[ifile]}")

        ##############################
        print(f"Downloading: {local_file}")
        ############################

        # The following prints on the sceen the entire
        # text of the request
        print("Request remote file: ", remote_file)
        if (not (os.path.exists(local_file))):
            # Download de remopte file
            ierr = download_grb_file(remote_file, local_file)
            print('dowloading error= ', ierr)
            if ierr == 0:  # successeful downloading
                print(f"Requested file downloaded in {local_file}")
            else:  # unsuccesseful downloading
                print(f'File {remote_file} not downloaded!')
                not_downloaded_files.append(remote_file)
        else:
            print('Archivo ya descargado')
    print(f"Failed to download files: [not_downloaded_files]")
   
    return True


def download_ftp(output_dir: str, inidate: str):
    """ Esta funcion es de bakcup y se ejecuta si y
        sólo si hay errores en grib_filter
    """

    inidate = int(inidate)
    date = inidate//100
    fci = inidate-date*100

    # Defines connection timeoutcd
    socket.setdefaulttimeout(30)

    # day="%4.4i%2.2i%2.2i" % (day.year,day.month,day.day)
    # # structure definition
    day = "%8.8i" % (date)
    fciA = "%2.2i" % fci

    # gfs.20201026/06/gfs.t06z.pgrb2.0p25.f027
    print("Date and hour of GFS forecast initial time: ", day, fci)
    dir_gfs_name = f"gfs.{day}/{fciA}/"

    # Full list of requested files
    list_remote_files = []
    list_files_local = []

    for iter_num_file in range(0, int(CANT_FILES_REQUESTED)):

        hf = iter_num_file*DHOUR
        hfA2 = "%3.3i" % hf

        file_name_base = f"gfs.t{fciA}z.pgrb2.0p25.f{hfA2}"

        remote_file = f"{dir_gfs_name}{file_name_base}"
        local_file = f"GFS_{day}{fciA}+{hfA2}.grib2"

        list_remote_files.append(remote_file)
        list_files_local.append(local_file)

        print("********************************")
        print(remote_file)
        print("********************************")
        print(local_file)
        print("********************************")

    ok = download(output_dir, list_remote_files, list_files_local, BASE_FTP)
    if ok:
        print("Descarga secundaria ok")
    else:
        print("---------------- FALLARON LOS DOS SERVERS ----------------")


def main():
    parser = argparse.ArgumentParser(
        description='descarga_GFS025.py --ini=inidate --output=output',
        epilog="script de descarga de GFS0925")

    parser.add_argument("--ini", type=int, dest="inidate", help="init date",
                        required=True)
    parser.add_argument("--out", dest="output", help="directories where \
                        downloaded files are stored and (optionally) archived",
                        required=True)
    parser.add_argument("--model", dest="model", help="if its gfs/gefs/gefs05",
                        required=True)
    parser.add_argument("--nhours", dest="nhours", help="hours of simultation \
                        to download", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    if not args.inidate or not args.output:
        print("The parameter is required.  Nothing to do!")
    else:
        list_remote_files, list_files_local = get_list_gfs(args.inidate,
                                                           args.model,
                                                           args.nhours)
        ok = download(args.output, list_remote_files, list_files_local)

        if ok:
            exit()
        else:
            download_ftp(args.output, args.inidate)


if __name__ == "__main__":
    main()
