#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/8 上午8:19
# @Author  : Hou Rong
# @Site    :
# @File    : mongo_task.py
# @Software: PyCharm
import pymongo
import datetime
import json
import hashlib
import toolbox.Date
from data_source import MysqlSource
from patched_mongo import mongo_patched_insert
from logger import get_logger

toolbox.Date.DATE_FORMAT = "%Y%m%d"

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']

logger = get_logger("insert_mongo_task")

base_data_final_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'BaseDataFinal'
}

offset = 0
pre_offset = 0


def insert_mongo(data):
    global offset
    global pre_offset
    res = mongo_patched_insert(data)
    logger.info("[update offset][offset: {}][pre offset: {}]".format(offset, pre_offset))
    offset = pre_offset
    logger.info("[insert info][ offset: {} ][ {} ]".format(offset, res))


def get_tasks():
    global offset
    global pre_offset
    query_sql = '''SELECT
      source,
      pic_md5,
      file_md5,
      info
    FROM hotel_images
    ORDER BY source,source_id
    LIMIT {},999999999999999;'''.format(offset)

    for source, file_name, file_md5, info in MysqlSource(db_config=base_data_final_config, table_or_query=query_sql,
                                                         size=10000, is_table=False,
                                                         is_dict_cursor=False):
        pre_offset += 1
        if not info:
            yield source, file_name, file_md5, 'mioji-hotel', 'hotel'

#     global offset
#     global pre_offset
#     query_sql = '''SELECT
#   source,
#   pic_md5,
#   pic_md5,
#   bucket_name,
#   info
# FROM poi_images
# ORDER BY source, sid
# LIMIT {}, 999999999999999;'''.format(offset)
#
#     for source, file_name, file_md5, bucket, info in MysqlSource(db_config=base_data_final_config,
#                                                                  table_or_query=query_sql,
#                                                                  size=10000, is_table=False,
#                                                                  is_dict_cursor=False):
#         pre_offset += 1
#         if not info:
#             if 'attr' in bucket:
#                 # bucket_name = 'mioji-attr'
#                 continue
#             elif 'rest' in bucket:
#                 bucket_name = 'mioji-rest'
#                 # continue
#             elif 'shop' in bucket:
#                 # bucket_name = 'mioji-shop'
#                 continue
#             else:
#                 continue
#             yield source, file_name, file_md5, bucket_name, 'poi'


def _insert_mongo_task():
    _count = 0
    data = []
    for source, file_name, file_md5, bucket_name, _type in get_tasks():
        _count += 1
        task_info = {
            'worker': 'proj.tasks.p_hash_calculate',
            'queue': 'file_downloader',
            'routing_key': 'file_downloader',
            'task_name': 'img_calc_p_hash_{0}'.format(source),
            'args': {
                "source": source,
                "_type": _type,
                "bucket_name": bucket_name,
                "file_name": file_name,
                "file_md5": file_md5,
            },
            'priority': 10,
            'running': 0,
            'used_times': 0,
            'finished': 0,
            'utime': datetime.datetime.now()
        }
        task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
        data.append(task_info)
        if _count % 1000 == 0:
            insert_mongo(data)
            data = []

    if data:
        insert_mongo(data)


def insert_mongo_task():
    while True:
        try:
            _insert_mongo_task()
            break
        except Exception as exc:
            logger.exception(msg="[insert mongo task exc]", exc_info=exc)


if __name__ == '__main__':
    insert_mongo_task()
