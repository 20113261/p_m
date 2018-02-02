#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/30 下午9:37
# @Author  : Hou Rong
# @Site    : 
# @File    : move_data.py
# @Software: PyCharm
from data_source import MysqlSource
from service_platform_conn_pool import base_data_final_pool
from my_logger import get_logger

logger = get_logger("move_data")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'ServicePlatform'
}


def insert_data(data):
    update_sql = '''REPLACE INTO poi_images (file_name, source, sid, url, pic_size, bucket_name, url_md5, pic_md5, 
    `use`, part, date, info) VALUE (%(file_name)s, %(source)s, %(sid)s, %(url)s, %(pic_size)s, %(bucket_name)s, %(url_md5)s, 
    %(pic_md5)s, %(use)s, %(part)s, %(date)s, %(info)s);'''
    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany(update_sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.debug("[move data][total: {}][execute: {}]".format(len(data), _res))


def move_img_data(source_table_name):
    query_sql = '''SELECT
  file_name,
  source,
  sid,
  url,
  pic_size,
  bucket_name,
  url_md5,
  pic_md5,
  `use`,
  part,
  date,
  info
FROM {};'''.format(source_table_name)
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
            logger.debug("[table_name: {}][move data][count: {}]".format(source_table_name, _count))
    if data:
        insert_data(data)


if __name__ == '__main__':
    table_names = ['images_total_qyer_20171120a', 'images_total_qyer_20171120a_bak',
                   'images_total_qyer_20171120a_bak_2', 'images_total_qyer_20171120a_bak_3',
                   'images_total_qyer_20171201a', 'images_total_qyer_20171201a_bak']

    for each_table_name in table_names:
        move_img_data(source_table_name=each_table_name)
