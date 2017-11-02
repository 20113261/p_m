#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/2 下午3:22
# @Author  : Hou Rong
# @Site    : 
# @File    : patched_mongo.py
# @Software: PyCharm
import mock
import pymongo
import patched_mongo_insert

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']


def mongo_patched_insert(data):
    with mock.patch('pymongo.collection.Collection._insert', patched_mongo_insert.Collection._insert):
        result = collections.insert(data, continue_on_error=True)
        return result
