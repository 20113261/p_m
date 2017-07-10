#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/9 下午10:44
# @Author  : Hou Rong
# @Site    : 
# @File    : get_latest_10_task.py
# @Software: PyCharm
import pymongo
import datetime

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['Task']['FullSite']

if __name__ == '__main__':
    for line in collections.find().sort('select_time', 1).limit(10):
        _id = line['_id']
        mid = line['mid']
        website_url = line['website_url']

        collections.update_one(
            {'_id': _id},
            {
                '$set': {
                    'select_time': datetime.datetime.now()
                }
            }, upsert=False)
        print(mid, website_url)
