#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/30 下午9:37
# @Author  : Hou Rong
# @Site    : 
# @File    : move_data.py
# @Software: PyCharm
from data_source import MysqlSource
from service_platform_conn_pool import base_data_final_pool
from logger import get_logger

logger = get_logger("move_data")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'tmp'
}


def insert_data(data):
    update_sql = '''INSERT IGNORE INTO poi_images (file_name, source, sid, url, pic_size, bucket_name, url_md5, pic_md5, `use`)
VALUE (%(file_name)s, 'online', %(sid)s, %(url)s, %(pic_size)s, %(bucket_name)s, %(url_md5)s, %(pic_md5)s, %(use)s);'''
    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany(update_sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.debug("[move data][total: {}][execute: {}]".format(len(data), _res))


def move_img_data():
    query_sql = '''SELECT
  file_name,
  sid,
  url,
  pic_size,
  bucket_name,
  url_md5,
  pic_md5,
  `use`,
  source,
  rank,
  fixrank,
  status,
  date
FROM attr_bucket_relation;'''
    data = []
    _count = 0
    for line in MysqlSource(poi_ori_config, table_or_query=query_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=True):
        _count += 1
        data.append(line)
        if len(data) == 1000:
            insert_data(data)
            data = []
            logger.debug("[move data][count: {}]".format(_count))
    if data:
        insert_data(data)


if __name__ == '__main__':
    move_img_data()
