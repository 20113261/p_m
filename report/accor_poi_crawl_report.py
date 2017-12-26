#!/usr/bin/env python
# -*- coding:utf-8 -*-
from gevent import monkey

monkey.patch_all()
from gevent import pool
import pymongo

MongoTask = pymongo.MongoClient(host='10.10.231.105')
hotelList = pymongo.MongoClient(host='10.10.213.148')
import json
from collections import defaultdict
import csv
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


def get_task_id():
    MongoTask_collection_Name = 'Task_Queue_hotel_list_TaskName_list_hotel_ihg_20171222a'
    MongoTask_collection = MongoTask['MongoTask'][MongoTask_collection_Name]
    _finish_results = defaultdict(list)
    _no_finish_results = defaultdict(list)
    results = MongoTask_collection.find({},
                                        {'task_token': True, 'args': True, 'finished': True, 'list_task_token': True})
    for result in results:

        task_token = result.get('task_token')
        city_id = result.get('args', {}).get('city_id')
        country_id = result.get('args', {}).get('country_id')
        suggest = json.loads(result.get('args', {}).get('suggest'))
        city_name = suggest.get('label')
        list_task_token = result.get('list_task_token')
        finished = result.get('finished')
        if finished == 1:
            _finish_results[(finished, country_id, city_id, city_name, list_task_token)].append(task_token)
        elif finished == 0:
            _no_finish_results[(finished, country_id, city_id, city_name, list_task_token)].append(task_token)
    return _finish_results, _no_finish_results


def gevent_query_data(keys, values):
    MongoTask_collection_Name = 'Task_Queue_hotel_list_TaskName_list_hotel_ihg_20171222a'
    hotelList_collection = hotelList['data_result']['HotelList']
    save_result = defaultdict(list)
    for value in values:
        temp = defaultdict(dict)
        max_crawl = 0
        max_filter = 0
        hotellist_results = hotelList_collection.find(
            {'collections': MongoTask_collection_Name, 'task_id': value}).hint([('collections', 1), ('task_id', 1)])
        for hotellist in hotellist_results:
            crawl_list = hotellist.get('result', {}).get('hotel', [])
            filter_list = hotellist.get('result', {}).get('holiday_filter', [])
            if len(crawl_list) >= max_crawl:
                max_crawl = len(crawl_list)

            if len(filter_list) >= max_filter:
                max_filter = len(filter_list)
            temp[value]['max_crawl'] = max_crawl
            temp[value]['max_filter'] = max_filter
        print "处理后的结果：", (keys[0], keys[1], keys[2], keys[3], keys[4], value, max_crawl, max_filter)
        save_result[(keys[0],keys[1],keys[2],keys[3],keys[4])].append(temp)

    for keys,values in save_result.items():
        if keys[0] == 1:
            with open('finish_report.csv', 'a+') as finish:
                writer = csv.writer(finish)
                values_str = json.dumps(values)
                writer.writerow((keys[0],keys[1],keys[2],keys[3],keys[4],values_str))
        else:
            with open('no_finish_report.csv', 'a+') as no_finish:
                writer = csv.writer(no_finish)
                values_str = json.dumps(values)
                writer.writerow((keys[0], keys[1], keys[2], keys[3], keys[4], values_str))


def handler_result(results):
    accor_pool = pool.Pool(100)
    for keys, values in results.items():
        accor_pool.apply_async(gevent_query_data, args=(keys, values))
    accor_pool.join()


def count_crawl_result():
    with open('finish_report.csv', 'w+') as finish:
        writer = csv.writer(finish)
        writer.writerow(('finish', 'country_id', 'city_id', 'city_name', 'list_task_token', 'task_token', 'max_crawl',
                         'max_filter'))
    with open('no_finish_report.csv', 'w+') as no_finish:
        writer = csv.writer(no_finish)
        writer.writerow(('finish', 'country_id', 'city_id', 'city_name', 'list_task_token', 'task_token', 'max_crawl',
                         'max_filter'))
    finish_result, no_finish_result = get_task_id()
    handler_result(finish_result)
    handler_result(no_finish_result)


if __name__ == "__main__":
    count_crawl_result()
