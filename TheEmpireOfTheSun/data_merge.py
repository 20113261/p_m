#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/5 下午8:45
# @Author  : Hou Rong
# @Site    : 
# @File    : data_merge.py
# @Software: PyCharm
import dataset
import pymongo
from Common.MiojiSimilarCityDict import is_legal, key_modify
from collections import defaultdict

client = pymongo.MongoClient(host='10.10.231.105')

if __name__ == '__main__':
    db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/private_data?charset=utf8')
    private_db = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.149.146/private_data?charset=utf8')
    base_data_db = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8')

    target_table = db['hotel_private_test']
    key_match = defaultdict(set)

    # 初始化数据存储
    collections = client['PrivateData']['TheEmpire']

    # 去重公有酒店，私有酒店对应关系
    for line in db['the_empire_of_the_sun'].all():
        mioji_hotel_id = line['mioji_hotel_id']
        private_data_id = line['private_data_id']
        if mioji_hotel_id and private_data_id:
            # 存储对应关系
            key_match[private_data_id].add(mioji_hotel_id)

    # 数据合并过程
    _count = 0
    for k, v in key_match.items():
        private_data_id = k
        mioji_hotel_id = v.pop()
        _count += 1
        # 初始化合并变量
        merged_data = {}
        already_merged_key = set()

        # 初始化需要合并的数据
        private_data = list(private_db.query('''SELECT * FROM hotel WHERE uid='{0}';'''.format(private_data_id)))[0]
        mioji_data = list(base_data_db.query('''SELECT * FROM hotel WHERE uid='{0}';'''.format(mioji_hotel_id)))[0]

        # 私有数据合并
        for k, v in private_data.items():
            if is_legal(v):
                merged_data[k] = v
                already_merged_key.add(k)

        for k, v in mioji_data.items():
            # 向私有数据融合内容
            if k in private_data.keys():
                if k not in already_merged_key:
                    if is_legal(v):
                        merged_data[k] = v
                        # target_table.insert(merged_data)
                        collections.save(merged_data)
    print(_count)
