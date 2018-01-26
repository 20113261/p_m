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
from service_platform_conn_pool import fetchall, source_info_pool

TaskClient = pymongo.MongoClient(host='10.10.231.105')
RespClient = pymongo.MongoClient(host='10.10.213.148')

TaskCollectionName = 'Task_Queue_poi_list_TaskName_list_total_qyer_20180122b'
# TaskCollectionName = 'Task_Queue_poi_list_TaskName_list_total_qyer_20171209a'

TaskCollections = TaskClient['MongoTask'][TaskCollectionName]
RespCollections = RespClient['data_result']['qyer_list']
# RespCollections = RespClient['data_result']['Qyer20171214a']


def task_resp_info():
    __total_dict = {}
    __resp_dict = defaultdict(set)
    for line in RespCollections.find({'collections': TaskCollectionName}).sort(
            [('used_times', 1)]):
        __total_dict[line['task_id']] = line['total_num']['attr']+line['total_num']['rest']+line['total_num']['shop']+line['total_num']['acti']

        # result loop
        for each in line['result']:
            __resp_dict[line['task_id']].add(each[1]+each[3])
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


def generate_c_url_map_dict():
    __dict = {}
    sql = '''SELECT
  city.id,
  city.name,
  city.grade,
  ota_location.suggest
FROM ota_location, city
WHERE ota_location.source = 'qyer' AND ota_location.city_id = city.id;'''
    for line in fetchall(source_info_pool, sql):
        __dict[line[3]] = (line[0], line[1], line[2])
    return __dict

def test_v(data,k):
    #print(k)
    #print(data)
    return len(data)

def generate_table():
    _l_items = []
    report_dict = generate_report()
    city_info_dict = generate_c_url_map_dict()
    for k, v in report_dict.items():
        c_info = city_info_dict.get(v[0], ('', '', 100))
        _id, _name, _grade = c_info

        if int(_grade) == -1:
            _grade = 100

        _l_items.append(
            (
                k,
                _grade,
                _id,
                _name,
                v[0],
                '|'.join(v[1]),
                '|'.join(map(lambda x: str(x), v[2])),
                max(v[2]) if v[2] else None,
                test_v(v[3],v[0])
            )
        )

    __table = pandas.DataFrame(
        columns=['list_task_token', 'city_grade', 'city_id', 'city_name', 'city_url', 'task_id',
                 'total_num_key', 'max_total_num', 'total_crawled'],
        data=_l_items
    )

    __table['crawled_diff'] = __table['max_total_num'] - __table['total_crawled']
    return __table.sort_values(by=['city_grade', 'city_id'])


if __name__ == '__main__':
    file_name = '/tmp/qyer_total_crawl_report_{}.csv'.format(TaskCollectionName)
    report_table = generate_table()
    report_table.to_csv(file_name)
    print(file_name)