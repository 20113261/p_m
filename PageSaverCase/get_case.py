#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/5 下午12:21
# @Author  : Hou Rong
# @Site    : 
# @File    : get_case.py
# @Software: PyCharm
import pymongo

if __name__ == '__main__':
    client = pymongo.MongoClient(host='10.10.231.105')
    collections = client['PageSaver']['hotel_base_data_agoda']

    for line in collections.find({
        'source_id': '807465'
    }):
        print(line)
