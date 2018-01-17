#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/1/15 下午6:24
# @Author  : Hou Rong
# @Site    : 
# @File    : mongo_count_distinct.py
# @Software: PyCharm
import pymongo
import pandas
from collections import defaultdict
from sqlalchemy import create_engine

client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
collections = client['Data']['veriflight']
count_res = client['Data']['flight_hot']

res_d = defaultdict(set)
for line in collections.find({}):
    try:
        zf = line['data']['data']['zf']
        res_d[line['iata_code']].add(len(zf))
    except Exception:
        res_d[line['iata_code']].add(0)

engine = create_engine('mysql+pymysql://10.119.')
table = pandas.read_sql()
for k, v in res_d.items():
    # print(k, '->', v.pop())
    count_res.save({
        'iata_code': k,
        'num': int(v.pop())
    })
