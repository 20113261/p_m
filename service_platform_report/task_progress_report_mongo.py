#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/28 下午2:49
# @Author  : Hou Rong
# @Site    : 
# @File    : task_progress_report_mongo.py
# @Software: PyCharm
import pymongo
import time
import datetime
import dataset
import json
from collections import defaultdict
from my_logger import get_logger

logger = get_logger("task_progress_mongo")

client = pymongo.MongoClient(host='10.10.231.105')
db = client['MongoTask']


def _each_task_progress(collections):
    dt = datetime.datetime.now()
    product_count = defaultdict(int)

    start = time.time()
    for each_task_name in collections.distinct('task_name', {}):
        if not isinstance(each_task_name, str):
            continue
        task_list = each_task_name.split('_')
        if len(task_list) != 4:
            logger.debug("[unknown task name][name: {}]".format(each_task_name))
            continue

        task_type, crawl_type, task_source, task_tag = task_list

        # task 名称，一批任务
        task_all = collections.count({'task_name': each_task_name}, hint=[('task_name', 1), ])
        # 已完成任务
        task_done = collections.count({'task_name': each_task_name, 'finished': 1},
                                      hint=[('task_name', 1), ('finished', 1)])
        # 已完成任务并且重试超过最大重试次数
        task_final_failed = collections.count({'task_name': each_task_name, 'finished': 0, 'used_times': {'$gte': 6}},
                                              hint=[('task_name', 1), ('finished', 1), ('used_times', 1)])

        product_count[(task_tag, crawl_type.title(), task_source.title(), task_type.title(), "All")] = task_all
        product_count[(task_tag, crawl_type.title(), task_source.title(), task_type.title(), "Done")] = task_done
        product_count[
            (task_tag, crawl_type.title(), task_source.title(), task_type.title(), "FinalFailed")] = task_final_failed

        if task_type == 'list':
            # 已完成的任务，通过 task_token 去重后
            list_task_has_data = len(
                collections.find({'task_name': each_task_name, 'finished': 1},
                                 hint=[('task_name', 1), ('finished', 1)]).distinct("list_task_token"))
            product_count[
                (task_tag, crawl_type.title(), task_source.title(), task_type.title(), "CityDone")] = list_task_has_data
            logger.debug(" ".join(
                map(lambda x: str(x), [each_task_name, task_all, task_done, task_final_failed, list_task_has_data])))
        else:
            logger.debug(" ".join(map(lambda x: str(x), [each_task_name, task_all, task_done, task_final_failed])))

    logger.debug('[get info][takes: {}]'.format(time.time() - start))

    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    product_table = db['serviceplatform_product_mongo_summary']
    for key, value in product_count.items():
        task_tag, crawl_type, task_source, task_type, report_key = key
        data = {
            'tag': task_tag,
            'source': task_source,
            'crawl_type': crawl_type,
            'type': task_type,
            'report_key': report_key,
            'num': value,
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }

        try:
            product_table.upsert(data, keys=['tag', 'source', 'crawl_type', 'type', 'report_key', 'date'],
                                 ensure=None)
            logger.debug("[final data][{}]".format(json.dumps(data, indent=4, sort_keys=True)))
        except Exception as exc:
            logger.exception(msg="[update task progress table exception]", exc_info=exc)
    db.commit()


def task_progress_report_main():
    for each_collections in db.collection_names():
        if str(each_collections).startswith('Task_Queue_'):
            collections = db[each_collections]
            try:
                _each_task_progress(collections=collections)
            except Exception as exc:
                logger.exception(msg="[error collections: {}]".format(each_collections), exc_info=exc)


if __name__ == '__main__':
    task_progress_report_main()
