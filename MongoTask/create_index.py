#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/8 下午8:46
# @Author  : Hou Rong
# @Site    : 
# @File    : create_index.py
# @Software: PyCharm
import pymongo


def create_indexes():
    client = pymongo.MongoClient(host='10.10.231.105')
    collections = client['MongoTask']['Task']
    collections.create_index([('finished', 1)])
    collections.create_index([('priority', -1), ('used_times', 1), ('utime', 1)])
    collections.create_index([('queue', 1), ('finished', 1), ('running', 1)])
    collections.create_index([('queue', 1), ('finished', 1), ('running', 1), ('used_times', 1)])
    collections.create_index([('queue', 1), ('finished', 1), ('used_times', 1), ('running', 1), ])

    collections.create_index([('queue', 1), ('finished', 1), ('utime', 1)])
    collections.create_index([('priority', -1), ('used_times', 1), ('utime', 1)])
    collections.create_index([('priority', -1), ('used_times', 1), ('utime', 1)])
    collections.create_index([('priority', -1), ('used_times', 1), ('utime', 1)])
