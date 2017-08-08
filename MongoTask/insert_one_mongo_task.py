#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/6 下午9:42
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_one_mongo_task.py
# @Software: PyCharm
import pymongo
import datetime
import json
import hashlib

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']

if __name__ == '__main__':
    task_info = {
        'worker': 'worker',
        'queue': 'test',
        'routing_key': 'routing_key',
        'args': {
            'a': 2,
            'b': 3,
            'c': 4,
            'd': 446
        },
        'priority': 5,
        'finished': 0,
        'used_times': 0,
        'utime': datetime.datetime.now()
    }
    task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
    collections.save(task_info)
