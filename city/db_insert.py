#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 下午7:44
# @Author  : Hou Rong
# @Site    : 
# @File    : db_insert.py
# @Software: PyCharm
import pandas
import dataset
import copy
from city.config import base_path,config
import json
from my_logger import get_logger
import pymysql
pymysql.install_as_MySQLdb()
import csv

import traceback
from collections import defaultdict
def shareAirport_insert(config, path, fname):
    select_sql = "select iata_code,name,name_en,belong_city_id,map_info,status,inner_order from airport where id=%s"
    update_sql = "insert ignore into airport(iata_code,name,name_en,belong_city_id,map_info,status,inner_order,city_id) values(%s,%s,%s,%s,%s,%s,%s,%s)"
    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    save_result = []
    with open(path+fname,'r+') as airport:
        reader = csv.DictReader(airport)
        for row in reader:
            cursor.execute(select_sql,(row['airport_id']))
            result = list(cursor.fetchone())
            result.append(row['city_id'])
            print("result:",result)
            save_result.append(tuple(result))
            if len(save_result) >= 200:
                cursor.executemany(update_sql,save_result)
                conn.commit()
                save_result = []
        else:
            cursor.executemany(update_sql, save_result)
            conn.commit()

def from_file_airport_insert(config,param,airport_paths):
    path = ''.join([base_path,str(param),'/'])
    update_sql = "insert ignore into airport(iata_code,name,name_en,city_id,belong_city_id,map_info,status,time2city_center,inner_order) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    _count = 0
    save_result = []
    logger = get_logger('step',path)
    for airport_path in airport_paths:
        logger.debug("函数名：{0},入机场文件名：{1}".format(from_file_airport_insert.__name__, airport_path))
        with open(path+airport_path,'r+') as airport:
            reader = csv.DictReader(airport)
            for row in reader:
                _count += 1
                logger.debug(row)
                save_result.append((row['iata_code'],row['name'],row['name_en'],row['city_id'],row['belong_city_id'],row['map_info'],row['status'],row['time2city_center'],row['inner_order']))
                if len(save_result) >= 2000:
                    cursor.executemany(update_sql,save_result)
                    conn.commit()
                    save_result = []

    if save_result:
        cursor.executemany(update_sql,save_result)
        conn.commit()
        save_result = []
    return _count
    # db = dataset.connect('mysql+pymysql://{user}:{password}@{host}:3306/{db}?charset=utf8'.format(**config))
    # airport_table = db['airport']
    # table = pandas.read_csv(path + 'share_airport.csv')
    # for i in range(len(table)):
    #     line = table.iloc[i]
    #     if line['airport_id'] != 'error':
    #         _count += 1
    #         data_line = airport_table.find_one(id=int(line['airport_id']))
    #         new_data = copy.deepcopy(data_line)
    #         new_data['city_id'] = int(line['city_id'])
    #         new_data.pop('id')
    #         new_data.pop('time2city_center')
    #         airport_table.upsert(new_data, keys=['city_id', 'iata_code'])
    # db.commit()

if __name__ == '__main__':
    temp_config = config
    temp_config['db'] = 'add_city_686'
    param = '686'
    airport_paths = ['add_new_share_airport.csv','add_new_airport.csv']
    from_file_airport_insert(temp_config,param,airport_paths)
