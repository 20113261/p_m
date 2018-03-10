#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

from city.field_check import check_repeat_airport,check_repeat_city,city_field_check,airport_field_check,airport_must_write_field,city_must_write_field
import pymysql
pymysql.install_as_MySQLdb()
import zipfile
from city.config import config,base_path,OpCity_config
import os
import sys
from collections import defaultdict
from call_city_project.city_step_one import task_start_one
import json
import traceback
def get_zip_path(param):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    select_sql = "select path1 from city_order  where id=%s"
    path = ''
    try:
        cursor.execute(select_sql,(param,))
        path = cursor.fetchone()[0]
    except Exception as e:
        conn.rollback()
    return path

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report2=%s,step2=%s where id=%s"
    update_sql_after = "update city_order set step3=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def task_start():
    try:
        param = sys.argv[1]
        task_start_one(param)
        zip_path = get_zip_path(param)
        file_name = zip_path.split('/')[-1]
        zip_path = ''.join([base_path, file_name])
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
            if '新增城市.xlsx' == child_file:
                city_path = path
            elif '新增机场.xlsx' == child_file:
                airport_path = path
            elif os.path.isdir(path):
                picture_path = path
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''

        func_and_args = {
            check_repeat_city: (city_path,param),
            # check_repeat_airport: (airport_path,param),
            city_must_write_field: (city_path,param),
            airport_must_write_field: (airport_path,param),
            city_field_check: (city_path, param,picture_path),
            airport_field_check: (airport_path,param)
        }

        flag = 1
        for func, args in func_and_args.items():
            if flag:
                check_field_path = func(*args)
                if check_field_path and flag:
                    temp_path = ''.join([base_path, check_field_path])
                    os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path, param))
                    save_path.append(check_field_path)
                    flag = 0

        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        csv_path = ';'.join(save_path)
        if flag:
            update_step_report('',param,1,0)
        else:
            update_step_report(csv_path,param,-1,0)
    except Exception as e:
        print(traceback.format_exc())
        csv_path = ';'.join(save_path)
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report(csv_path,param,-1,0)
if __name__ == "__main__":
    task_start()
