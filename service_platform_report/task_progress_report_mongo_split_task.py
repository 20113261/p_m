#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/20 上午10:12
# @Author  : Hou Rong
# @Site    : 
# @File    : task_progress_report_mongo_split_task.py
# @Software: PyCharm
import pymongo
import time
import datetime
import dataset
import json
import re
from collections import defaultdict
from my_logger import get_logger

logger = get_logger("task_progress_mongo_split_task")

client = pymongo.MongoClient(host='10.10.231.105')
db = client['MongoTask']


def _each_task_progress(collections):
    dt = datetime.datetime.now()
    product_count = defaultdict(int)
    '''
    :key (task_name, task_type)
    :value (all, done, final_failed, city_all, city_done)
    '''

    start = time.time()
    for each_task_name in collections.distinct('task_name', {}):
        if str(each_task_name).startswith('list'):
            task_type = 'list'
        else:
            task_type = 'others'

        # task 名称，一批任务
        task_all = collections.count({'task_name': each_task_name}, hint=[('task_name', 1), ])
        # 已完成任务
        task_done = collections.count({'task_name': each_task_name, 'finished': 1},
                                      hint=[('task_name', 1), ('finished', 1)])
        # 已完成任务并且重试超过最大重试次数
        task_final_failed = collections.count({'task_name': each_task_name, 'finished': 0, 'used_times': {'$gte': 6}},
                                              hint=[('task_name', 1), ('finished', 1), ('used_times', 1)])

        if task_type == 'list':
            # 已完成的任务，通过 list_task_token 去重后
            # 城市总数
            list_task_all = len(
                collections.find({'task_name': each_task_name},
                                 hint=[('task_name', 1)]).distinct("list_task_token"))
            # 城市有数据比率
            list_task_has_data = len(
                collections.find({'task_name': each_task_name, 'finished': 1},
                                 hint=[('task_name', 1), ('finished', 1)]).distinct("list_task_token"))
        else:
            list_task_all = list_task_has_data = 0

        # 统计对象添加数值
        product_count[(each_task_name, task_type)] = (
            task_all, task_done, task_final_failed, list_task_all, list_task_has_data)

        logger.debug(" ".join(
            map(lambda x: str(x),
                [each_task_name, task_all, task_done, task_final_failed, list_task_all, list_task_has_data])))

    logger.debug('[get info][takes: {}]'.format(time.time() - start))

    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    product_table = db['serviceplatform_product_mongo_split_task_summary']
    for keys, values in product_count.items():
        task_name, task_type = keys
        _all, _done, _final_failed, _city_all, _city_done = values
        data = {
            'task_name': task_name,
            'type': task_type,
            'all': _all,
            'done': _done,
            'final_failed': _final_failed,
            'city_all': _city_all,
            'city_done': _city_done,
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }

        try:
            product_table.upsert(data, keys=['task_name', 'date', 'date'],
                                 ensure=None)
            logger.debug("[final data][{}]".format(json.dumps(data, indent=4, sort_keys=True)))
        except Exception as exc:
            logger.exception(msg="[update task progress table exception]", exc_info=exc)
    db.commit()


def task_progress_report_split_task_main():
    for each_collections in db.collection_names():
        if str(each_collections).startswith('Task_Queue_'):
            collections = db[each_collections]
            try:
                _each_task_progress(collections=collections)
            except Exception as exc:
                logger.exception(msg="[error collections: {}]".format(each_collections), exc_info=exc)


if __name__ == '__main__':
    task_progress_report_split_task_main()
