import os

verbose = False
debug = False

base_url = 'https://bdhm.ohmc.ar/api/'

token = os.getenv('TOKEN')
usr = os.getenv('USER')
secret = os.getenv('SECRET')

gfs_info = {'name': 'GFS', 'param': 'Unique', 'run': '00:00', 'version': '1.0',
            'grid_resolution': '[0.25º]x[0.25º]', 'global_model': 'GFS'}
