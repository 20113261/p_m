#!usr/bin/env python
# -*- coding:utf-8 -*-

import csv
import pymysql
pymysql.install_as_MySQLdb()
import sys
import os
from city.config import base_path
from my_logger import get_logger
from collections import defaultdict
import json
import traceback
logger = get_logger('city')
config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'db': 'base_data',
    'charset': 'utf8'
}

def revise_pictureName(picute_path,config,param):
    path = ''.join([base_path, str(param), '/'])
    city_map = {}
    with open(path+'city_id.csv','r+') as city:
        reader = csv.DictReader(city)
        for row in reader:
            city_map[str(row['city_id_number'])] = str(row['city_id'])
    file_names = os.listdir(picute_path)
    if '.DS_Store' in file_names:
        file_names.remove('.DS_Store')
    for file_name in file_names:
        real_name,extend_name = os.path.splitext(file_name)
        city_number_id = real_name.split('_')[0]
        if city_number_id == '2009297':
            print(city_number_id)
        if city_map.get(city_number_id):
            new_file_name = ''.join([city_map.get(str(city_number_id)),'_',real_name.split('_')[1],extend_name])
            old_name = '/'.join([picute_path,file_name])
            new_name = '/'.join([picute_path,new_file_name])
            os.rename(old_name,new_name)
    return True

if __name__ == "__main__":
    revise_pictureName('/search/service/nginx/html/MioaPyApi/store/opcity/图片','','706')
