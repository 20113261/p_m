#!/usr/bin/env python
# -*- coding:utf-8 -*-


from city.update_city_pic import update_city_pic
from city.config import config,base_path
import os
import sys
import zipfile

import pymysql
pymysql.install_as_MySQLdb()
def get_zip_path(param):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    select_sql = "select path1 from city_order  where id=%s"
    path = ''
    try:
        cursor.execute(select_sql,(param,))
        path = cursor.fetchone()[0]
    except Exception as e:
        conn.rollback()
    return path
def task_start():
    param = sys.argv[1]
    zip_path = get_zip_path(param)
    zip = zipfile.ZipFile(zip_path)
    file_name = zip.filename.split('.')[0].split('/')[-1]
    path = ''.join([base_path, str(param), '/'])
    if path.endswith('/'):
        file_path = ''.join([path, file_name])
    else:
        file_path = '/'.join([path, file_name])
    file_list = os.listdir(file_path)
    for child_file in file_list:
        path = '/'.join([file_path, child_file])
        if os.path.isdir(path):
            picture_path = path
            break
    update_city_pic(picture_path,config)

if __name__ == "__main__":
    task_start()