#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/11 上午10:22
# @Author  : Hou Rong
# @Site    : 
# @File    : qyer_poi_crawl_report.py
# @Software: PyCharm
import pymongo
import pandas
from collections import defaultdict
from logger import get_logger

TaskClient = pymongo.MongoClient(host='10.10.231.105')
RespClient = pymongo.MongoClient(host='10.10.213.148')

# TaskCollectionName = 'Task_Queue_poi_list_TaskName_list_total_qyer_20171214a'
TaskCollectionName = 'Task_Queue_poi_list_TaskName_list_total_qyer_20171209a'

TaskCollections = TaskClient['MongoTask'][TaskCollectionName]
# RespCollections = RespClient['data_result']['qyer']
RespCollections = RespClient['data_result']['Qyer20171214a']


def task_resp_info():
    __total_dict = {}
    __resp_dict = defaultdict(set)
    for line in RespCollections.find({'collections': TaskCollectionName}).sort(
            [('used_times', 1)]):
        __total_dict[line['task_id']] = line['total_num']['attr']

        # result loop
        for each in line['result']:
            __resp_dict[line['task_id']].add(each[0])
    return __total_dict, __resp_dict


def generate_report():
    __dict = {}
    total_dict, resp_dict = task_resp_info()
    for line in TaskCollections.find({}).sort([('list_task_token', 1)]):
        city_url = line['args']['city_url']
        list_task_token = line['list_task_token']
        task_token = line['task_token']

        total_num = total_dict.get(task_token)
        crawled = resp_dict.get(task_token)

        # if total_num is None:
        #     continue
        # if crawled is None:
        #     continue

        if list_task_token not in __dict:
            __dict[list_task_token] = [city_url, [], [], set()]

        __dict[list_task_token][1].append(task_token)
        if total_num:
            __dict[list_task_token][2].append(total_num)
        if crawled:
            __dict[list_task_token][3].update(crawled)

    return __dict


def generate_table():
    _l_items = []
    report_dict = generate_report()
    for k, v in report_dict.items():
        _l_items.append(
            (
                k,
                v[0],
                '|'.join(v[1]),
                '|'.join(map(lambda x: str(x), v[2])),
                max(v[2]) if v[2] else None,
                len(v[3])
            )
        )

    __table = pandas.DataFrame(
        columns=['list_task_token', 'city_url', 'task_id', 'total_num_key', 'max_total_num', 'total_crawled'],
        data=_l_items
    )

    __table['crawled_diff'] = __table['max_total_num'] - __table['total_crawled']
    return __table


if __name__ == '__main__':
    report_table = generate_table()
    report_table.to_csv('/tmp/qyer_total_crawl_report_{}.csv'.format(TaskCollectionName))
