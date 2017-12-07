#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/8 上午8:19
# @Author  : Hou Rong
# @Site    :
# @File    : mongo_task.py
# @Software: PyCharm
from data_source import MysqlSource
from logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask

logger = get_logger("insert_mongo_task")


def get_tasks():
    f = open('/tmp/img_list(7).csv')
    for line in f:
        sid, _, url = line.strip().split(',')
        yield sid, url


def insert_task():
    with InsertTask(worker='proj.total_tasks.images_task', queue='file_downloader', routine_key='file_downloader',
                    task_name='image_wanle_huantaoyou', source='huantaoyou', _type='FileDownloader',
                    priority=11) as it:
        for sid, url in get_tasks():
            args = {
                'source': "huantaoyou",
                'source_id': sid,
                'target_url': url,
                'bucket_name': 'mioji-wanle',
                'file_prefix': 'huantaoyou',
                'is_poi_task': True,
                'need_insert_db': True,
            }
            it.insert_task(args)


if __name__ == '__main__':
    insert_task()
