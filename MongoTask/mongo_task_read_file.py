#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/9 下午4:19
# @Author  : Hou Rong
# @Site    : 
# @File    : mongo_task_read_file.py
# @Software: PyCharm
# !/usr/bin/env python
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

if __name__ == '__main__':
    f = open('/tmp/all_img_1.txt')
    data = []
    _count = 0
    for target_url in f:
        _count += 1
        task_info = {
            # 'worker': 'proj.qyer_poi_tasks.qyer_poi_task',
            # 'worker': 'proj.tasks.get_lost_attr',
            # 'worker': 'proj.tasks.get_lost_shop',
            'worker': 'proj.file_downloader_task.file_downloader',
            'queue': 'file_downloader',
            'routing_key': 'file_downloader',
            # 'task_name': 'daodao_shop',
            'task_name': 'file_download',
            'args': {
                # 'target_url': target_url,
                # 'city_id': 'NULL',
                'url': target_url.strip(),
                'file_type': 'img',
                'file_path': '/search/nfs/image/img_itrip',
                'need_filter': 'NO',
                'file_split': 'NO'
            },
            'priority': 5,
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
