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

spider_data_base_data_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'base_data'
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
    query_sql = '''SELECT uid
FROM hotel
ORDER BY uid
LIMIT {}, 999999999999999;'''.format(offset)

    for line in MysqlSource(db_config=spider_data_base_data_config,
                            table_or_query=query_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=False):
        pre_offset += 1
        yield line[0]


def _insert_mongo_task():
    _count = 0
    data = []
    for uid in get_tasks():
        _count += 1
        task_info = {
            'worker': 'proj.merge_tasks.hotel_img_merge',
            'queue': 'merge_task',
            'routing_key': 'merge_task',
            'task_name': "merge_hotel_image_20171108_40",
            'args': {
                'uid': uid,
                'min_pixels': '400000',
                'target_table': 'hotel_40'
            },
            'priority': 11,
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
