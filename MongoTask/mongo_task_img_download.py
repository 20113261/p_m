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
import pymysql
import toolbox.Date

toolbox.Date.DATE_FORMAT = "%Y%m%d"

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']
conn = pymysql.connect(host='10.19.118.147', user='reader', password='mioji1109', charset='utf8', db='source_info')


def task_enter():
    f = open('/tmp/task_file_new')
    for line in f:
        url, file_name = line.strip().split('\t')
        if url != 'pic_url':
            yield url, file_name


if __name__ == '__main__':
    data = []
    _count = 0
    _finished = 0
    exc_set = set()
    for url, file_name in task_enter():
        _count += 1
        task_info = {
            'worker': 'proj.tasks.get_images',
            'queue': 'file_downloader',
            'routing_key': 'file_downloader',
            'task_name': 'file_downloader_{0}'.format("lost_img"),
            'args': {
                'source': 'lost_img',
                'source_id': '',
                'target_url': url,
                'part': 'lost_img',
                'file_path': '/data/nfs/image/0910',
                'desc_path': '/data/nfs/image/0910_filter',
                'is_poi_task': False,
                'need_insert_db': False,
                'special_file_name': file_name
            },
            'priority': 10,
            'running': 0,
            'used_times': 0,
            'finished': 0,
            'utime': datetime.datetime.now()
        }
        task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
        data.append(task_info)
        if _count % 10000 == 0:
            try:
                collections.insert(data, continue_on_error=True)
            except Exception as e:
                pass
            data = []
            print("Now", _count)
            print("Finished", _finished)

    try:
        collections.insert(data, continue_on_error=True)
    except Exception as e:
        pass
    data = []
    print(exc_set)
    print("Now", _count)
    print("Finished", _finished)
