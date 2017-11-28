#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/8 下午12:52
# @Author  : Hou Rong
# @Site    : 
# @File    : update_task.py
# @Software: PyCharm
import pymongo

client = pymongo.MongoClient(host='10.10.231.105')
db = client['MongoTask']
# collections = client['MongoTask']['Task']
for c_name in filter(lambda x: x.startswith("Task_Queue_hotel_list_TaskName") and x.endswith("20171127a"),
                     db.collection_names()):
    collections = db[c_name]
    # city task
    # print(c_name, collections.update({}, {
    #     "$set": {
    #         'finished': 0,
    #         'data_count': [],
    #         'task_result': [],
    #         'date_index': 0
    #     }
    # }, multi=True))

    print(c_name, collections.update({}, {
        "$set": {
            'finished': 0,
            'running': 0,
            'used_times': 0
        }
    }, multi=True))
