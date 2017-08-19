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

conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='shop_merge')
# conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='rest_merge')
cursor = conn.cursor()
if __name__ == '__main__':
    # cursor.execute('''select distinct id,url from attr where source='daodao';''')
    cursor.execute('''select distinct id,url from shop where source='daodao';''')
    data = []
    _count = 0
    _finished = 0
    exc_set = set()
    for _, target_url in cursor.fetchall():
        _count += 1
        task_info = {
            # 'worker': 'proj.qyer_poi_tasks.qyer_poi_task',
            # 'worker': 'proj.tasks.get_lost_attr',
            'worker': 'proj.tasks.get_lost_shop',
            # 'worker': 'proj.tasks.get_lost_rest',
            'queue': 'poi_task_1',
            'routing_key': 'poi_task_1',
            'task_name': 'daodao_shop',
            # 'task_name': 'daodao_rest',
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
        try:
            collections.save(task_info)
            _finished += 1
        except Exception as exc:
            exc_set.add(str(exc))

        if _count % 1000 == 0:
            print("Now", _count)
            print("Finished", _finished)
    print(exc_set)
            #     data.append(task_info)
            #     if _count % 10000 == 0:
            #         print(_count)
            #         try:
            #             collections.insert(data, continue_on_error=True)
            #             data = []
            #         except Exception:
            #             pass
            #
            # try:
            #     collections.insert(data, continue_on_error=True)
            # except Exception:
            #     pass
