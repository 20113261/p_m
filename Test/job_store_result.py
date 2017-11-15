#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/14 下午6:15
# @Author  : Hou Rong
# @Site    : 
# @File    : job_store_result.py
# @Software: PyCharm
import pymongo
import base64

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['SchedulerJob']['data_report_jobs']
base64.decodebytes(collections.find_one()['job_state'].encode())