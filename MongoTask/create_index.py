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
    collections.create_index([('queue', 1), ('finished', 1), ('used_times', 1), ('priority', 1)])
    collections.create_index([('queue', 1), ('finished', 1), ('used_times', 1), ('priority', 1), ('running', 1)])
    collections.create_index([('running', 1), ('utime', 1)])
    collections.create_index([('running', 1), ('utime', -1)])
    collections.create_index([('task_name', 1)])
    collections.create_index([('task_name', 1), ('finished', 1)])
    collections.create_index([('task_name', 1), ('finished', 1), ('used_times', 1)])
    collections.create_index([('task_name', 1), ('list_task_token', 1)])
    collections.create_index([('task_token', 1)], unique=True)
    collections.create_index([('utime', 1)])
    collections.create_index([('finished', 1)])


if __name__ == '__main__':
    create_indexes()
