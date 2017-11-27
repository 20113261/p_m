#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/27 下午4:30
# @Author  : Hou Rong
# @Site    : 
# @File    : baidu_map_report.py
# @Software: PyCharm
import pymongo
import pandas
from collections import defaultdict

client = pymongo.MongoClient(host='10.10.231.105')
city_collections = client['CrawlData']['TravelAgencyCity']
collections = client['CrawlData']['TravelAgency']

count_dict = defaultdict(int)
for each in collections.find({}):
    _cid = each['cid']
    count_dict[_cid] += 1

data = []
for each in city_collections.find({}):
    data.append({
        'c_name': each['c_name'],
        'cid': each['cid'],
        'c_level': each['c_level'],
        'total': each['result']['total'],
        'aladdin_res_num': each['result']['aladdin_res_num'],
        'crawl': count_dict[str(each['cid'])]
    })

table = pandas.DataFrame(columns=['c_name', 'cid', 'c_level', 'total', 'aladdin_res_num', 'crawl'], data=data)
table = table.sort_values(by=['c_level', 'cid'])
print(table)
table.to_csv('/tmp/check_result.csv')
