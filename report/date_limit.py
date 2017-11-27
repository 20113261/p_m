#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/27 下午8:44
# @Author  : Hou Rong
# @Site    : 
# @File    : date_limit.py
# @Software: PyCharm
import re
import pymongo

client = pymongo.MongoClient(host='10.10.231.105')


def date_limit(coll_name):
    source = re.findall('City_Queue_hotel_list_TaskName_city_hotel_(\w+)_20171122a', coll_name)[0]
    collections = client['MongoTask'][coll_name]
    for line in collections.find({}):
        print('#' * 30 + line['list_task_token'] + '#' * 30)
        for i in line['data_count']:
            tid, date_index, d_count, d_insert, u_times, t_res = i
            if t_res:
                print(tid, date_index, d_count, d_insert)
        print('#' * 100)


if __name__ == '__main__':
    # date_limit('City_Queue_hotel_list_TaskName_city_hotel_agoda_20171122a')
    db = client['MongoTask']
    for each_name in filter(
            lambda x: x.startswith('City_Queue_hotel_list_TaskName_city_hotel') and x.endswith('20171122a'),
            db.collection_names()):
        print('-' * 100)
        date_limit(each_name)
