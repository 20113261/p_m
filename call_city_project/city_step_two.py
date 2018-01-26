#!/usr/bin/env python
# -*- coding:utf-8 -*-

from city.field_check import check_repeat_airport,check_repeat_city,city_field_check,airport_field_check,airport_must_write_field,city_must_write_field
import pymysql
pymysql.install_as_MySQLdb()
import zipfile
from city.config import config,base_path
import os
import sys
from collections import defaultdict
import json
import traceback
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
    save_path = []
    file_name = zip.filename.split('.')[0].split('/')[-1]
    path = ''.join([base_path, str(param), '/'])
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
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        city_repeat_path = check_repeat_city(city_path,param)
        if city_repeat_path:
            save_path.append(city_repeat_path)
        airport_repeat_path = check_repeat_airport(airport_path,param)
        if airport_repeat_path:
            save_path.append(airport_repeat_path)
        city_must_path = city_must_write_field(city_path,param)
        if city_must_path:
            save_path.append(city_must_path)

        airport_must_path = airport_must_write_field(airport_path,param)
        if airport_must_path:
            save_path.append(airport_must_path)
        city_field_path = city_field_check(city_path, param)
        if city_field_path:
            save_path.append(city_field_path)
        airport_field_path = airport_field_check(airport_path,param)
        if airport_field_path:
            save_path.append(airport_field_path)
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        csv_path = ';'.join(save_path)
        update_step_report(csv_path,param,1)
    except Exception as e:
        csv_path = ';'.join(save_path)
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report(csv_path,param,-1)
if __name__ == "__main__":
    task_start()