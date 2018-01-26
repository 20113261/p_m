#!/usr/bin/env python
# -*- coding:utf-8 -*-

from city.field_check import new_airport_insert
from city.add_city import read_file
from city.share_airport import update_share_airport
from city.city_map_ciytName import revise_pictureName
from city.update_city_pic import update_city_pic
from city.db_insert import shareAirport_insert
from city.config import config,base_path
import os
import sys
import zipfile
from collections import defaultdict
import json
import traceback
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

def update_step_report(csv_path,param,step):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    update_sql = "update city_order set report2=%s,step2=%s where id=%s"
    try:
       cursor.execute(update_sql,(csv_path,step,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def task_start():
    param = sys.argv[1]
    zip_path = get_zip_path(param)
    zip = zipfile.ZipFile(zip_path)
    file_name = zip.filename.split('.')[0].split('/')[-1]
    path = ''.join([base_path, str(param), '/'])
    save_path = []
    if path.endswith('/'):
        file_path = ''.join([path, file_name])
    else:
        file_path = '/'.join([path, file_name])
    file_list = os.listdir(file_path)
    for child_file in file_list:
        path = '/'.join([file_path, child_file])
        if '新增城市.xlsx' in child_file:
            city_path = path
        elif '新增机场.xlsx' in child_file:
            airport_path = path
        elif os.path.isdir(path):
            picture_path = path

    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        city_insert_path = read_file(city_path,config,param)
        if city_insert_path:
            save_path.append(city_insert_path)
        city_map_path = revise_pictureName(picture_path,config,param)
        if city_map_path:
            save_path.append(city_map_path)
        city_pic_path = update_city_pic(picture_path,config,param)
        if city_pic_path:
            save_path.append(city_pic_path)
        share_airport_path = update_share_airport()
        if share_airport_path:
            save_path.extend(share_airport_path)

        shareAirport_insert(config)
        new_airport_insert(config,param)
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        csv_path = ';'.join(save_path)
        update_step_report(csv_path, param, 1)
    except Exception as e:
        csv_path = ';'.join(save_path)
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report(csv_path, param, -1)


if __name__ == "__main__":
    task_start()