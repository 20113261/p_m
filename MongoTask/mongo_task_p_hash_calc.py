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

toolbox.Date.DATE_FORMAT = "%Y%m%d"

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']

base_data_final_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'BaseDataFinal'
}


def insert_mongo(data):
    try:
        collections.insert(data, continue_on_error=True)
    except Exception as e:
        pass


def get_tasks():
    query_sql = '''SELECT
      source,
      pic_md5,
      file_md5
    FROM hotel_images;'''

    for source, file_name, file_md5 in MysqlSource(db_config=base_data_final_config, table_or_query=query_sql,
                                                   size=10000, is_table=False,
                                                   is_dict_cursor=False):
        yield source, file_name, file_md5, 'mioji-hotel', 'hotel'


def insert_mongo_task():
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
            'utime': datetime.datetime.now()
        }
        task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
        data.append(task_info)
        if _count % 1000 == 0:
            insert_mongo(data)
            data = []

    if data:
        insert_mongo(data)


if __name__ == '__main__':
    insert_mongo_task()
