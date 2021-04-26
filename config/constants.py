import os

verbose = False
debug = False

base_url = 'https://bdhm.ohmc.ar/api/'

token = os.getenv('TOKEN')
usr = os.getenv('USER')
secret = os.getenv('SECRET')

variables = {
        'T2P': 'gfs_mean_region_temperature',
        'wdir10': 'gfs_mean_region_wdir',
        'wspd10': 'gfs_mean_region_wspd'
    }

gfs_info = {'name': 'GFS', 'param': 'Unique', 'run': '00:00', 'version': '1.0',
            'grid_resolution': '[0.25º]x[0.25º]', 'global_model': 'GFS'}
