#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/8 上午8:19
# @Author  : Hou Rong
# @Site    :
# @File    : mongo_task.py
# @Software: PyCharm
import os
import pymongo
import datetime
import json
import hashlib
import toolbox.Date

toolbox.Date.DATE_FORMAT = "%Y%m%d"

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']


def task_enter():
    parent_path = '/data/nfs/image'
    ls_parent_path = '/search/image'
    path_list = [
        'download_img_filter_0905_0', 'download_img_filter_0905_1',
        # 'download_img_filter_0905_2', 'download_img_filter_0907',
        # 'download_img_filter_0907_2', 'download_img_filter_0907_3',
        # 'download_img_filter_0907_4', 'download_img_filter_0907_5'
    ]
    _count = 0
    for _each_path in path_list:
        for _each in os.listdir(os.path.join(ls_parent_path, _each_path)):
            _count += 1
            yield os.path.join(parent_path, _each_path, _each)
    print(_count)


if __name__ == '__main__':
    data = []
    _count = 0
    _finished = 0
    exc_set = set()
    for each_path in task_enter():
        _count += 1
        task_info = {
            'worker': 'proj.tasks.get_hotel_images_info',
            'queue': 'file_downloader',
            'routing_key': 'file_downloader',
            'task_name': 'get_hotel_images_info_0914',
            'args': {
                'path': each_path,
                'part': '45',
                'desc_path': ''
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
