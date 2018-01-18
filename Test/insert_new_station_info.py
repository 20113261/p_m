#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/21 下午8:10
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_new_station_info.py
# @Software: PyCharm
from warnings import filterwarnings

import pymongo
import pymysql
from service_platform_conn_pool import service_platform_pool
from logger import get_logger

filterwarnings('ignore', category=pymysql.err.Warning)
logger = get_logger("insert_new_station_info")

client = pymongo.MongoClient(host='10.10.213.148')

collections = client['base_data']['station_new']


def insert_db(data):
    # return
    sql = '''INSERT IGNORE INTO NewStation.station_src (station, src_city, src_country, map_info, station_city_map_info, status, belong_city_id, station_code_from_europeRail, src_city_code, src_station_code)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    conn = service_platform_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info('[total: {}][insert: {}]'.format(len(data), _res))


_count = 0
ds = []
# for line in collections.find({}):
for line in collections.find({"inventory": ''}):
    _count += 1
    map_info = line['map_info']
    if not map_info:
        map_info = 'NULL'
    data = (
        line['station_name'],
        line['city_name'],
        line['country_code'],
        map_info,
        line['city_map_info'],
        'Open',
        line['city_id'],
        '{}&{}'.format(line['station_code'], line['city_code']),
        line['city_code'],
        line['station_code']
    )
    ds.append(data)
    if len(ds) == 1000:
        insert_db(ds)
        ds = []
if ds:
    insert_db(ds)
