#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/6 上午11:05
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_db.py
# @Software: PyCharm
import pymongo
import dataset

client = pymongo.MongoClient(host='10.10.231.105')

if __name__ == '__main__':
    db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/private_data?charset=utf8')
    target_table = db['hotel_private_new']
    # 初始化数据存储
    collections = client['PrivateData']['TheEmpire']
    for line in collections.find():
        del line['_id']
        target_table.insert(line)
        print(line)
