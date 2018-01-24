#!/usr/bin/env python
# -*- coding:utf-8 -*-


from city.share_airport import update_share_airport
from city.config import config,base_path,zip_path
import os
import sys
import zipfile


def task_start():
    param = sys.argv[1]
    config['db'] = ''.join(['add_city_', str(param)])
    zip = zipfile.ZipFile(zip_path)
    file_name = zip.filename.split('.')[0].split('/')[-1]
    if base_path.endswith('/'):
        file_path = ''.join([base_path, file_name])
    else:
        file_path = '/'.join([base_path, file_name])
    file_list = os.listdir(file_path)
    for child_file in file_list:
        path = '/'.join([file_path, child_file])
        if '城市' in child_file:
            city_path = path
            break
    update_share_airport()

if __name__ == "__main__":
    task_start()