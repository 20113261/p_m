#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/8 下午12:52
# @Author  : Hou Rong
# @Site    : 
# @File    : remove_task.py
# @Software: PyCharm
import pymongo

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']

collections.remove({"queue": "poi_task_1"})
