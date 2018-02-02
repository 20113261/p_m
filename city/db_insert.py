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
from city.config import base_path
import json
from my_logger import get_logger
import pymysql
pymysql.install_as_MySQLdb()
import csv
logger = get_logger('city')
import traceback
from collections import defaultdict
def shareAirport_insert(config,param):
    path = ''.join([base_path, str(param), '/'])
    select_sql = "select iata_code,name,name_en,belong_city_id,map_info,status,inner_order from airport where id=%s"
    update_sql = "insert into airport(idta_code,name,name_en,belong_city_id,map_info,status,inner_order,city_id) values(%s,%s,%s,%s,%s,%s,%s,%s)"
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    _count = 0
    save_result = []
    with open(path+'share_airport.csv','r+') as airport:
        reader = csv.DictReader(airport)
        for row in reader:
            cursor.execute(select_sql,(row['airport_id']))
            result = cursor.fetchone()
            result = list(result).append(row['city_id'])
            print("result:",result)
            save_result.append(tuple(result))
            if len(save_result) >= 200:
                cursor.execute(update_sql,save_result)
                conn.commit()
                save_result = []
        else:
            cursor.execute(update_sql, save_result)
            conn.commit()
            save_result = []
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
    shareAirport_insert()
