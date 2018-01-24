#!/usr/bin/env python
# -*- coding:utf-8 -*-


from city.field_check import city_must_write_field
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
    city_must_write_field(city_path)

if __name__ == "__main__":
    task_start()