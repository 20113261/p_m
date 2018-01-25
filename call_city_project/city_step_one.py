#!/usr/bin/env python
# -*- coding:utf-8 -*-

from city.field_check import check_repeat_city
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
    finally:
        conn.close()
    return path

def update_step_report(csv_path,param):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    update_sql = "update city_order set report2=%s where id=%s"
    select_sql = "select report2 from city_order where id=%s"
    try:
        cursor.execute(select_sql,(param,))
        report2 = cursor.fetchone()
        if not report2:
            cursor.execute(update_sql,(csv_path,param))
            conn.commit()
        else:
            report2 = ';'.join([report2,csv_path])
            cursor.execute(update_sql,(report2,param))
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
    path = ''.join([base_path,str(param),'/'])
    if path.endswith('/'):
        file_path = ''.join([path, file_name])
    else:
        file_path = '/'.join([path, file_name])
    file_list = os.listdir(file_path)
    for child_file in file_list:
        path = '/'.join([file_path, child_file])
        if '新增城市.xlsx' in child_file:
            city_path = path
            break

    csv_path = check_repeat_city(city_path,param)
    update_step_report(csv_path,param)
if __name__ == "__main__":
    task_start()