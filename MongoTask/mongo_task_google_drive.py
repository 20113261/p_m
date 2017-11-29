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

toolbox.Date.DATE_FORMAT = "%Y%m%d"

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']


def task_enter():
    _count = 0
    for line in open('/search/hourong/task/target_url_1128'):
        s_l = line.strip().split('###')
        yield s_l
        _count += 1
    print(_count)


# def task_enter():
#     _count = 0
#     # google_long_drive_url(1)
#     for line in open('/Users/hourong/Downloads/google_long_drive_url(1)'):
#         yield line.strip()
#         # s_l = line.strip().split('###')
#         # yield s_l
#         _count += 1
#     print(_count)


if __name__ == '__main__':
    data = []
    _count = 0
    _finished = 0
    exc_set = set()
    result_set = set()
    for url, flag, table_name in task_enter():
        _count += 1
        task_info = {
            'worker': 'proj.tasks.craw_html',
            'queue': 'hotel_task',
            'routing_key': 'hotel_task',
            'task_name': 'google_drive_1018',
            'args': {
                'url': url,
                'flag': flag,
                'table_name': table_name
            },
            'priority': 6,
            'running': 0,
            'used_times': 0,
            'finished': 0,
            'utime': datetime.datetime.now()
        }
        result_set.add((url, flag, table_name))
        task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
        data.append(task_info)
        if _count % 1000 == 0:
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



# large task

# data = []
# _count = 0
# _finished = 0
# exc_set = set()
# for url, flag, table_name in task_enter():
#     _count += 1
#     task_info = {
#         'worker': 'proj.tasks.craw_html',
#         'queue': 'hotel_task',
#         'routing_key': 'hotel_task',
#         'task_name': 'google_drive_0905',
#         'args': {
#             'url': url,
#             'flag': flag,
#             'table_name': table_name
#         },
#         'priority': 6,
#         'running': 0,
#         'used_times': 0,
#         'finished': 0,
#         'utime': datetime.datetime.now()
#     }
#     task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
#     data.append(task_info)
#     if _count % 10000 == 0:
#         try:
#             collections.insert(data, continue_on_error=True)
#         except Exception as e:
#             pass
#         data = []
#         print("Now", _count)
#         print("Finished", _finished)
#
# try:
#     collections.insert(data, continue_on_error=True)
# except Exception as e:
#     pass
# data = []
# print(exc_set)
# print("Now", _count)
# print("Finished", _finished)
