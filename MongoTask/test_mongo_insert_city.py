#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/20 下午12:28
# @Author  : Hou Rong
# @Site    : 
# @File    : test_mongo_insert_city.py
# @Software: PyCharm

import pymongo
import datetime
import json
import hashlib
import pymysql
import toolbox.Date

toolbox.Date.DATE_FORMAT = "%Y%m%d"

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']
conn = pymysql.connect(host='10.19.118.147', user='reader', password='mioji1109', charset='utf8', db='source_info')


def task_enter():
    f = open('/tmp/task_file_new')
    for line in f:
        url, file_name = line.strip().split('\t')
        if url != 'pic_url':
            yield url, file_name


if __name__ == '__main__':
    task_info = {
        'task_name': 'city_total_qyer_20170929a',
        'list_task_token': '',
        'data_count': '',
        'date_array': [],
        'date_index': 0,
        'args': {

        },
        'priority': 10,
        'running': 0,
        'used_times': 0,
        'finished': 0,
        'utime': datetime.datetime.now()
    }
