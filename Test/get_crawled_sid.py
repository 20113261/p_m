#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/25 下午6:31
# @Author  : Hou Rong
# @Site    : 
# @File    : get_crawled_sid.py
# @Software: PyCharm
import pymongo
from bson import ObjectId

client = pymongo.MongoClient(host='10.10.213.148')
collections = client['data_result']['qyer']
r_set = set()
for line in collections.find({'task_id': 'f8223d3212f0ba9e1504abe30cfc6fc4'}):
    for each in line['result']:
        r_set.add(each[0])
print(len(r_set))
print(r_set)
