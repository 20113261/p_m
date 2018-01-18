#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/28 下午11:29
# @Author  : Hou Rong
# @Site    : 
# @File    : sid_count.py
# @Software: PyCharm
import pymongo

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task_Queue_hotel_detail_TaskName_detail_hotel_booking_20171127a']

sid_set = set()
count = 0
for line in collections.find({}):
    count += 1
    if count % 10000 == 0:
        print(count)
    sid_set.add(line['args']['source_id'])
print(count)
print(len(sid_set))
