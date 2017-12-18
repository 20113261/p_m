#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 下午9:34
# @Author  : Hou Rong
# @Site    : 
# @File    : select_and_update_table.py
# @Software: PyCharm
from data_source import MysqlSource
from service_platform_conn_pool import poi_ori_pool, poi_face_detect_pool, service_platform_pool, base_data_final_pool, source_info_pool
from logger import get_logger
from toolbox.Hash import encode

logger = get_logger("select_and_update_table")

poi_ori_config = {
    'host': '10.10.230.206',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'source_info'
}


def update_sql(data):
    sql = '''INSERT INTO ota_location (source, sid_md5, sid, suggest_type, suggest, city_id, country_id, s_city, s_region, s_country, s_extra, label_batch, others_info) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
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
    logger.info("[total: {}][execute: {}]".format(len(data), _res))


def get_task():
    sql = '''SELECT
  source,
  sid,
  suggest_type,
  suggest,
  city_id,
  country_id,
  s_city,
  s_region,
  s_country,
  s_extra,
  label_batch,
  others_info
FROM ota_location_bak_1215;'''
    data = []
    _count = 0
    for line in MysqlSource(poi_ori_config, table_or_query=sql,
                            size=2000, is_table=False,
                            is_dict_cursor=False):
        _count += 1
        new_line = list(line)
        new_line.insert(1, encode(line[1]))
        data.append(new_line)
        if len(data) == 1000:
            logger.info("[count: {}]".format(_count))
            update_sql(data)
            data = []
    update_sql(data)


if __name__ == '__main__':
    get_task()
