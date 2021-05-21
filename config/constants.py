import os

verbose = False
debug = False

base_url = 'https://bdhm.ohmc.com.ar/api/'

token = os.getenv('TOKEN')
usr = os.getenv('USER')
secret = os.getenv('SECRET')

variables = {
        'T2P': 'gfs_mean_region_temperature',
        'wdir': 'gfs_mean_region_wdir',
        'wspd': 'gfs_mean_region_wspd'
    }

gfs_info = {'name': 'GFS', 'param': 'Unique', 'run': '00:00', 'version': '1.0',
            'grid_resolution': '[0.25º]x[0.25º]', 'global_model': 'GFS'}

EXTENT = [-64.0, -43.8, -60.0, -42.8]
KM_PER_DEGREE = 111.32
RESOLUTION = 1