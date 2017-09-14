#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/11 下午6:48
# @Author  : Hou Rong
# @Site    : 
# @File    : mongo_task_hotel_static_data.py
# @Software: PyCharm
import pymongo
import datetime
import hashlib
import json
import traceback

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['MongoTask']['Task']


def insert_daodao_mongo_new():
    collections_page_server = client['PageSaver']['hotelinfo_routine_expedia']
    _count = 0
    for line in collections_page_server.find():
        _count += 1
        task_id, city_id, source, url, source_id, _id = line['task_id'], line['city_id'], line['source'], line['url'], \
                                                        line['source_id'], line['_id']
        task_info = {
            'worker': 'proj.hotel_static_tasks.hotel_static_base_data',
            'queue': 'file_downloader',
            'routing_key': 'file_downloader',
            'task_name': 'expedia_hotels_0911_2',
            'args': {
                'parent_task_id': str(task_id),
                'task_name': 'hotelinfo_routine_expedia',
                'source': source,
                'source_id': source_id,
                'city_id': city_id,
                'hotel_url': url,
            },
            'priority': 10,
            'finished': 0,
            'used_times': 0,
            'running': 0,
            'utime': datetime.datetime.now()
        }
        task_info['task_token'] = hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()

        if _count % 10000 == 0:
            print(_count)
        try:
            collections.update({
                'task_token': hashlib.md5(json.dumps(task_info['args'], sort_keys=True).encode()).hexdigest()
            }, {'$set': task_info}, upsert=True)
        except Exception as exc:
            print('==========================0=======================')
            print(url, city_id)
            print(traceback.format_exc(exc))
            print('==========================1=======================')
    else:
        print(_count)


if __name__ == '__main__':
    insert_daodao_mongo_new()
