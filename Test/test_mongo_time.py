#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/8 下午11:47
# @Author  : Hou Rong
# @Site    : 
# @File    : test_mongo_time.py
# @Software: PyCharm
import pymongo
from bson import ObjectId

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']

if __name__ == '__main__':
    for line in collections.find({"_id": ObjectId("5a03257e659e75067079c4f7")}):
        print(line)
