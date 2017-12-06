#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/7 上午12:27
# @Author  : Hou Rong
# @Site    : 
# @File    : img_larger_than_90_update.py
# @Software: PyCharm
import json
import pymysql.cursors
from data_source import MysqlSource
from service_platform_conn_pool import base_data_final_pool
from logger import get_logger

logger = get_logger("img_larger_than_90_update")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'BaseDataFinal'
}


def update_sql(sid, data):
    sql = '''UPDATE poi_images
SET `use` = 0, info = %s
WHERE source = 'qyer' AND sid = '{}' AND file_name = %s;'''.format(sid)
    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany(sql, data)
    cursor.close()
    conn.close()
    logger.info("[sid: {}][total: {}][execute: {}]".format(sid, len(data), _res))


def get_file(sid):
    sql = '''SELECT
  file_name,
  pic_size,
  info
FROM poi_images
WHERE source = 'qyer' AND sid = '{}' AND `use` = 1;'''.format(sid)
    conn = base_data_final_pool.connection()
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql)
    file_pic_size = {}
    for line in cursor.fetchall():
        w, h = eval(line['pic_size'])
        _j_data = json.loads(line['info'])
        _j_data['down_reason'] = "图片数多余 90 张，下掉一部分"
        file_pic_size[(json.dumps(_j_data), line['file_name'])] = int(w) * int(h)
    cursor.close()
    conn.close()

    down_imgs = list(map(lambda x: x[0], sorted(file_pic_size.items(), key=lambda x: x[1], reverse=True)))[90:]
    update_sql(sid=sid, data=down_imgs)


def get_task():
    sql = '''SELECT sid
FROM poi_images
WHERE source = 'qyer' AND `use` = 1
GROUP BY sid
HAVING count(*) > 90;'''
    for line in MysqlSource(poi_ori_config, table_or_query=sql,
                            size=10000, is_table=False,
                            is_dict_cursor=True):
        get_file(line['sid'])


if __name__ == '__main__':
    get_task()
