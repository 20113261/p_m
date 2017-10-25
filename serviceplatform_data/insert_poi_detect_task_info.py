#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/25 上午9:49
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_poi_detect_task_info.py
# @Software: PyCharm
import pymysql
from warnings import filterwarnings
from service_platform_conn_pool import service_platform_pool, base_data_pool
from data_source import MysqlSource
from logger import get_logger

filterwarnings('ignore', category=pymysql.err.Warning)

logger = get_logger("insert_poi_detect_task_info")

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
    insert_sql = '''INSERT IGNORE INTO pic_detect_task (city_id, city_grade, poi_id, pic_name) VALUES (%s, %s, %s, %s);'''

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
            logger.debug(
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


def _get_per_table_task_info(table_name):
    global offset
    sql = '''SELECT
            {0}.city_id                    AS poi_city_id,
            {0}.source                     AS poi_sid,
            sid                            AS poi_source,
            file_name                      AS pic_name
          FROM BaseDataFinal.poi_images
            JOIN {0}
              ON BaseDataFinal.poi_images.source = {0}.source AND
                 BaseDataFinal.poi_images.sid = {0}.source_id
          WHERE city_id != 'NULL' and BaseDataFinal.poi_images.`use` != '0'
          LIMIT {1},999999999;'''.format(table_name, offset)
    data = []
    _count = 0
    for line in MysqlSource(service_platform_conf, table_or_query=sql, size=10000, is_table=False, is_dict_cursor=True):
        cid = line['poi_city_id']
        c_grade = cid2grade[cid]
        source = line['poi_source']
        sid = line['poi_sid']
        pic_name = line['pic_name']
        poi_id = '###'.join([source, sid])

        data.append((cid, c_grade, poi_id, pic_name))
        _count += 1
        offset += 1
        if len(data) == 2000:
            insert_task_data(data, _count)
            data = []
    insert_task_data(data, _count)


def get_per_table_task_info(table_name):
    global offset
    global cid2grade
    cid2grade = prepare_city_info()

    max_retry_times = 1000
    while max_retry_times:
        max_retry_times -= 1
        try:
            _get_per_table_task_info(table_name)
            break
        except Exception as exc:
            logger.exception(msg="[get task info error]", exc_info=exc)


def get_task_info():
    # detect list
    # get image
    local_conn = service_platform_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'ServicePlatform';''')
    # 强制要求按照 tag 的先后顺序排列
    list_tables = list(
        sorted(
            filter(lambda x: x.startswith('list_attr_daodao'),
                   map(lambda x: x[0],
                       local_cursor.fetchall()
                       )
                   ),
            key=lambda x: x.split('_')[-1]
        )
    )
    local_cursor.close()
    local_conn.close()

    for each_table_name in list_tables:
        get_per_table_task_info(each_table_name)


if __name__ == '__main__':
    get_task_info()
