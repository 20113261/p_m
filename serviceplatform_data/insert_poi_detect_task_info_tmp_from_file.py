#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/25 上午9:49
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_poi_detect_task_info.py
# @Software: PyCharm
import pymysql
import logging
from warnings import filterwarnings
from service_platform_conn_pool import service_platform_pool, base_data_pool
from logger import get_logger

filterwarnings('ignore', category=pymysql.err.Warning)

logger = get_logger("insert_poi_detect_task_info_from_img")
logger.setLevel(logging.DEBUG)

service_platform_conf = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'ServicePlatform'
}

offset = 0
cid2grade = None


def insert_task_data(data, _count):
    # 插入 pic detect task 数据
    insert_sql = '''INSERT IGNORE INTO pic_detect_task_test (city_id, city_grade, poi_id, pic_name) VALUES (%s, %s, %s, %s);'''

    max_retry_times = 3
    while max_retry_times:
        max_retry_times -= 1
        try:
            conn = service_platform_pool.connection()
            cursor = conn.cursor()
            _insert_count = cursor.executemany(insert_sql, data)
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(
                "[insert data][now count: {}][insert data: {}][insert_ignore_count: {}]".format(_count, len(data),
                                                                                                _insert_count))
            break
        except Exception as exc:
            logger.exception(msg="[run sql error]", exc_info=exc)


def prepare_city_info():
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT
  id,
  CASE WHEN grade != -1
    THEN grade
  ELSE 100 END AS grade
FROM city;''')
    _res = {line[0]: line[1] for line in cursor.fetchall()}
    cursor.close()
    conn.close()
    return _res


def prepare_poi_info():
    _dict = {}
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    _sql = '''SELECT
  id,
  city_id
FROM chat_attraction;'''
    cursor.execute(_sql)
    _count = 0
    for line in cursor.fetchall():
        _count += 1
        if _count % 10000 == 0:
            logger.debug("[prepare_pic_task: {}]".format(_count))
        _dict[line[0]] = line[1]
    cursor.close()
    conn.close()
    logger.debug("[prepare_pic_task: {}]".format(_count))
    return _dict


def get_info():
    _count = 0
    data = []
    f = open('/tmp/img_res_new')
    poi2cid = prepare_poi_info()
    cid2grade = prepare_city_info()
    _count += 1
    for line in f:
        l_task = line.strip().split('###')
        if len(l_task) != 2:
            logger.debug("[unknown task][task: {}]".format(line.strip()))
            continue
        poi_id, file_name = l_task
        cid = poi2cid.get(poi_id, '')
        if not cid:
            logger.debug("[unknown poi city][poi_id: {}]".format(poi_id))
            continue
        c_grade = cid2grade.get(cid, '')
        if not c_grade:
            logger.debug("[unknown city grade][poi_id: {}][city_id: {}]".format(poi_id, cid))
            continue
        data.append((cid, c_grade, poi_id, file_name))

        if len(data) == 2000:
            insert_task_data(data, _count)
            data = []
    if data:
        insert_task_data(data, _count)


if __name__ == '__main__':
    get_info()
