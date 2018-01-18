#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/26 下午8:16
# @Author  : Hou Rong
# @Site    : 
# @File    : MkContent.py
# @Software: PyCharm
from warnings import filterwarnings

import pymysql
from service_platform_conn_pool import base_data_pool, fetchall, new_service_platform_pool
from itertools import permutations
from logger import get_logger

logger = get_logger("train_content")
filterwarnings('ignore', category=pymysql.err.Warning)

offset = 0


def get_task():
    sql = '''SELECT
  DISTINCT
  city_id,
  src_city_code
FROM station_src, station_relation
WHERE
  station_src.src_station_code IS NOT NULL AND station_src.station_id = station_relation.station_id AND city_id != '' GROUP BY city_id;'''
    for line in fetchall(base_data_pool, sql):
        yield line


def insert_db(data):
    sql = '''INSERT IGNORE INTO workload_train (workload_key, content, source, dept_id, dest_id, pure_source, type) VALUES (%s, %s, %s, %s, %s, %s, %s);'''
    conn = new_service_platform_pool.connection()
    cursor = conn.cursor()
    res = cursor.executemany(sql, data)
    cursor.close()
    conn.close()
    logger.info("[offset: {}][total: {}][insert: {}]".format(offset, len(data), res))


def generate_task():
    """
    10001_10005_ctripRail,FRPAR&巴黎&10001&ITMIL&米兰&10005&,ctripRail,0,2,10001,10005,ctrip,rail,2017-05-22 17:04:43
    CPH_PAR_raileuropeRail,CPH&PAR&Copenhagen&Paris&,raileuropeRail,0,2,CPH,PAR,raileurope,rail,2016-12-13 14:59:33
    "PAR&FRMRS&LON&ITMIL&20180222"
    :return:
    """
    global offset
    info = list(get_task())
    res = []
    for (src_cid, src_code), (dst_cid, dst_code) in permutations(info, r=2):
        offset += 1
        data = (
            '{}_{}_raileuropeApiRail'.format(src_cid, dst_cid),
            '{}&{}&{}&{}&'.format(src_cid, src_code, dst_cid, dst_code),
            'raileuropeApiRail',
            src_cid,
            dst_cid,
            'raileuropeApi',
            'rail'
        )
        res.append(data)
        if len(res) == 10000:
            insert_db(res)
            res = []
    if res:
        insert_db(res)


if __name__ == '__main__':
    generate_task()
