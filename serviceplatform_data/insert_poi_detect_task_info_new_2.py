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
    # 'db': 'ServicePlatform'
    'db': 'poi_merge'
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


def _get_per_table_task_info():
    global offset
    # qyer 补充
    #     sql = '''SELECT
    #   attr_unid.city_id AS poi_city_id,
    #   attr_unid.source  AS poi_source,
    #   sid               AS poi_sid,
    #   file_name         AS pic_name
    # FROM BaseDataFinal.poi_images, poi_merge.attr_unid
    # WHERE
    #   attr_unid.source = 'qyer' AND BaseDataFinal.poi_images.source = 'qyer' AND BaseDataFinal.poi_images.`use` != '0' AND
    #   attr_unid.source = BaseDataFinal.poi_images.source AND attr_unid.source_id = BaseDataFinal.poi_images.sid
    #   LIMIT {0},999999999;'''.format(offset)

    # todo attr_unid
    # sql = '''SELECT
    #   attr_unid.city_id AS poi_city_id,
    #   attr_unid.source  AS poi_source,
    #   sid               AS poi_sid,
    #   file_name         AS pic_name
    # FROM BaseDataFinal.poi_images, poi_merge.attr_unid
    # WHERE BaseDataFinal.poi_images.`use` != '0' AND
    #   attr_unid.source = BaseDataFinal.poi_images.source AND attr_unid.source_id = BaseDataFinal.poi_images.sid
    #   LIMIT {0},999999999;'''.format(offset)


    # todo shop_unid
    sql = '''SELECT
      shop_unid.city_id AS poi_city_id,
      shop_unid.source  AS poi_source,
      sid               AS poi_sid,
      file_name         AS pic_name
    FROM BaseDataFinal.poi_images, poi_merge.shop_unid
    WHERE BaseDataFinal.poi_images.`use` != '0' AND
      shop_unid.source = BaseDataFinal.poi_images.source AND shop_unid.source_id = BaseDataFinal.poi_images.sid
      LIMIT {0},999999999;'''.format(offset)

    #     sql = '''SELECT
    #   BaseDataFinal.attr_final_20171222a.city_id AS poi_city_id,
    #   BaseDataFinal.attr_final_20171222a.source  AS poi_source,
    #   sid                                  AS poi_sid,
    #   file_name                            AS pic_name
    # FROM BaseDataFinal.poi_images, BaseDataFinal.attr_final_20171222a
    # WHERE BaseDataFinal.poi_images.`use` != '0' AND
    #       BaseDataFinal.attr_final_20171222a.source = BaseDataFinal.poi_images.source AND
    #       BaseDataFinal.attr_final_20171222a.id = BaseDataFinal.poi_images.sid LIMIT {0},999999999;'''.format(offset)
    data = []
    _count = 0
    for line in MysqlSource(service_platform_conf, table_or_query=sql, size=10000, is_table=False, is_dict_cursor=True):
        cid = line['poi_city_id']
        c_grade = cid2grade.get(cid, 100)
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


def get_per_table_task_info():
    global offset
    global cid2grade
    cid2grade = prepare_city_info()

    max_retry_times = 1000
    while max_retry_times:
        max_retry_times -= 1
        try:
            _get_per_table_task_info()
            break
        except Exception as exc:
            logger.exception(msg="[get task info error]", exc_info=exc)


if __name__ == '__main__':
    get_per_table_task_info()
