#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/13 下午4:48
# @Author  : Hou Rong
# @Site    : 
# @File    : get_total_task.py
# @Software: PyCharm
import pymongo
import copy

client = pymongo.MongoClient(host='10.10.213.148')
collections = client['data_result']['qyer']

res_set = set()
for line in collections.find({'task_id': 'bbb8cf20aa99d6df04fa79c836a32bcd'}):
    for each in line['result']:
        res_set.add(each[1])

for url in res_set:
    print(url)
