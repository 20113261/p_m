#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/17 上午10:23
# @Author  : Hou Rong
# @Site    : 
# @File    : unique_task_test.py
# @Software: PyCharm
import pymongo

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['Test']['UniqueTest']

if __name__ == '__main__':
    collections.save({
        'a': 1,
        'b': 2,
        'c': 3,
        'd': 123
    })
    collections.create_index(['a', 'b', 'c'])
