#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/11 上午10:59
# @Author  : Hou Rong
# @Site    : 
# @File    : ctrip_annotation.py
# @Software: PyCharm
import pymongo
import dataset
import json
from collections import defaultdict

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['Suggestions']['ctrip']

# db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/newspider?charset=utf8')
# target_table = db['hotel_suggestions_city_new']

db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.230.206/source_info?charset=utf8')
target_table = db['hotel_suggestions_city']

if __name__ == '__main__':
    city_id_data = defaultdict(set)

    for line in collections.find({'annotation': '0'}):
        city_id = line['city_id']
        data = line['data']['data']
        keyword = line['key_word']

        for d in data.split('@'):
            detail_list = d.split('|')
            if len(detail_list) == 13:
                if detail_list[2] == 'city':
                    if keyword in detail_list:
                        city_id_data[city_id].add('|'.join(detail_list[:-1]))

        if len(city_id_data[city_id]) == 0:
            for d in data.split('@'):
                detail_list = d.split('|')
                if len(detail_list) == 13:
                    if keyword in detail_list:
                        city_id_data[city_id].add('|'.join(detail_list[:-1]))

    for k, v in city_id_data.items():
        if len(v) != 0:
            # 插入新的 suggestion 数据
            target_table.upsert({
                'city_id': str(k),
                'source': 'ctrip',
                'suggestions': json.dumps(list(v)),
                'select_index': 1,
                'annotation': '0',
                'error': '{"code": 0}',
                'label_batch': '20170711a'
            }, keys=['source', 'city_id'])
    print(len(city_id_data))
