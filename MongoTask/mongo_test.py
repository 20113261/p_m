#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/20 下午12:12
# @Author  : Hou Rong
# @Site    : 
# @File    : mongo_test.py
# @Software: PyCharm
import pymongo
import pymongo.errors

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['Test']['Test123123']

if __name__ == '__main__':
    # collections.remove()
    try:
        collections.insert_many([
            {'a': 123},
            {'a': 222}, {'a': 123}, {'a': 333}, {'a': 345}, {'a': 55556}
        ], ordered=False)
    except pymongo.errors.DuplicateKeyError as exc:
        pass
