#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 下午9:34
# @Author  : Hou Rong
# @Site    : 
# @File    : select_and_update_table.py
# @Software: PyCharm
import json
from data_source import MysqlSource
from service_platform_conn_pool import poi_ori_pool, poi_face_detect_pool, service_platform_pool, base_data_final_pool, \
    source_info_pool
from logger import get_logger

logger = get_logger("select_and_update_table")

poi_ori_config = {
    'host': '10.10.230.206',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'source_info'
}


def update_sql(data, source):
    sql = '''UPDATE ota_location
SET suggest = %s
WHERE source = '{}' AND sid = %s;'''.format(source)
    conn = source_info_pool.connection()
    cursor = conn.cursor()
    try:
        _res = cursor.executemany(sql, data)
    except Exception:
        print(sql)
        raise Exception()
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[source: {}][total: {}][execute: {}]".format(source, len(data), _res))


def get_task(source):
    sql = '''SELECT
  sid,
  suggest
FROM ota_location
WHERE source = '{}' AND suggest_type = 2;'''.format(source)
    data = []
    _count = 0
    for line in MysqlSource(poi_ori_config, table_or_query=sql,
                            size=10000, is_table=False,
                            is_dict_cursor=False):
        _count += 1
        data.append((json.dumps(eval(line[1])), line[0]))
        if len(data) == 1000:
            logger.info("[source: {}][count: {}]".format(source, _count))
            update_sql(data, source)
            data = []
    update_sql(data, source)


if __name__ == '__main__':
    source_list = ['hotels', 'elong', 'agoda']
    for source in source_list:
        get_task(source)
