#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/28 下午2:49
# @Author  : Hou Rong
# @Site    : 
# @File    : task_progress_report_mongo.py
# @Software: PyCharm
import pymongo
import time
import logging
import datetime
import dataset
import json
from logging import getLogger, StreamHandler
from collections import defaultdict

logger = getLogger('task_progress_mongo')
logger.level = logging.DEBUG
handler = StreamHandler()
logger.addHandler(handler)

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']


def main():
    dt = datetime.datetime.now()
    product_count = defaultdict(int)
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    product_table = db['serviceplatform_product_mongo_summary']

    start = time.time()
    for each_task_name in collections.distinct('task_name', {}):
        task_list = each_task_name.split('_')
        if len(task_list) != 4:
            continue

        task_type, crawl_type, task_source, task_tag = task_list

        # task 名称，一批任务
        task_all = collections.count({'task_name': each_task_name})
        # 已完成任务
        task_done = collections.count({'task_name': each_task_name, 'finished': 1})
        # 已完成任务并且重试超过最大重试次数
        task_final_failed = collections.count({'task_name': each_task_name, 'finished': 0, 'used_times': {'$gte': 6}})

        product_count[(task_tag, crawl_type.title(), task_source.title(), task_type.title(), "All")] = task_all
        product_count[(task_tag, crawl_type.title(), task_source.title(), task_type.title(), "Done")] = task_done
        product_count[
            (task_tag, crawl_type.title(), task_source.title(), task_type.title(), "FinalFailed")] = task_final_failed

        if task_type == 'list':
            # 已完成的任务，通过 task_token 去重后
            list_task_has_data = len(
                collections.distinct('list_task_token', {'task_name': each_task_name, 'finished': 1}))
            product_count[
                (task_tag, crawl_type.title(), task_source.title(), task_type.title(), "CityDone")] = list_task_has_data
            logger.debug(" ".join(
                map(lambda x: str(x), [each_task_name, task_all, task_done, task_final_failed, list_task_has_data])))
        else:
            logger.debug(" ".join(map(lambda x: str(x), [each_task_name, task_all, task_done, task_final_failed])))

    logger.debug('get info takes: ' + str(time.time() - start))

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
        except Exception:
            pass

        print(json.dumps(data, indent=4, sort_keys=True))


if __name__ == '__main__':
    main()
