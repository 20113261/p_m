#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/19 上午9:25
# @Author  : Hou Rong
# @Site    : 
# @File    : get_bali_url.py
# @Software: PyCharm
import pymongo

client = pymongo.MongoClient(host='10.10.213.148')
collections = client['data_result']['Qyer20171214a']

s = set()
for line in collections.find({'task_id': "0c42e43a1e2246358b8b1937805b61d5"}):
    for each in line['result']:
        s.add(each[0])

print(len(s))
print(s)
