#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/4 上午9:40
# @Author  : Hou Rong
# @Site    : 
# @File    : get_qyer_bad_case.py
# @Software: PyCharm
import pymongo

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task_Queue_poi_detail_TaskName_detail_total_qyer_20171201a']
for line in collections.find({"finished": 0}):
    print(line['args']['target_url'])
