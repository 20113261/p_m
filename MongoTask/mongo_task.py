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

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']

conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='attr_merge')
cursor = conn.cursor()
if __name__ == '__main__':
    cursor.execute('''select distinct id,url from attr where source='qyer';''')
    data = []
    _count = 0
    for _, target_url in cursor.fetchall():
        _count += 1
        task_info = {
            'worker': 'proj.qyer_poi_tasks.qyer_poi_task',
            'queue': 'poi_task_2',
            'routing_key': 'poi_task_2',
            'args': {
                'target_url': target_url,
                'city_id': 'NULL',
            },
            'priority': 3,
            'finished': 0,
            'used_times': 0,
            'utime': datetime.datetime.now()
        }
        task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
        data.append(task_info)
        if _count % 10000 == 0:
            print(_count)
            try:
                collections.insert(data, continue_on_error=True)
                data = []
            except Exception:
                pass

    try:
        collections.insert(data, continue_on_error=True)
    except Exception:
        pass
