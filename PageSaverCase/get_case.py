#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/5 下午12:21
# @Author  : Hou Rong
# @Site    : 
# @File    : get_case.py
# @Software: PyCharm
import pymongo
import zlib

if __name__ == '__main__':
    client = pymongo.MongoClient(host='10.10.231.105')
    collections = client['PageSaver']['hotel_base_data_170612']

    print(zlib.decompress(list(collections.find({'source_id': '482499'}).limit(1))[0]['content']).decode())
    # print(list(collections.find({'source_id': '482499'}))[0]['task_id'])
