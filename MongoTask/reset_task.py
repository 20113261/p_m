#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/30 下午9:27
# @Author  : Hou Rong
# @Site    : 
# @File    : reset_task.py
# @Software: PyCharm
import pymongo
import time
from my_logger import get_logger

logger = get_logger("reset_mongo_task")

client = pymongo.MongoClient(host='10.10.231.105')
db = client['MongoTask']

for each_name in db.collection_names():
    if each_name.startswith('Task_Queue_hotel_detail_TaskName_') and each_name.endswith('20171127a'):
        if 'expedia' not in each_name:
            continue
        start = time.time()
        collections = db[each_name]
        res = collections.update(
            {'finished': 0},
            {
                '$set': {
                    'running': 0,
                    'used_times': 0
                }
            },
            multi=True
        )
        logger.info("[collections: {}][res: {}][takes: {}]".format(
            each_name,
            res,
            time.time() - start
        ))
