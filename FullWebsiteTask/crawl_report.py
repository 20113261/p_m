#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/10 下午2:07
# @Author  : Hou Rong
# @Site    : 
# @File    : crawl_report.py
# @Software: PyCharm
import pymongo
from collections import defaultdict

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['FullSiteSpider']['AttrFullSiteNew']

if __name__ == '__main__':
    img_dict = defaultdict(list)
    pdf_dict = defaultdict(list)

    _count = 0
    for line in collections.find():
        _count += 1
        if _count % 10000 == 0:
            print(_count)
        key = (line['parent_info']['id'], line['parent_url'])
        for link in line['img_url']:
            img_dict[key].append(link)
        for link in line['pdf_url']:
            pdf_dict[key].append(link)

    print(_count)

    print('img')
    _img_total = 0
    _img_unique_total = 0
    for k, v in img_dict.items():
        print('未去重', k, len(v))
        print('去重', k, len(set(v)))
        _img_total += len(v)
        _img_unique_total += len(set(v))
    print('total', _img_total)
    print('total_unique', _img_unique_total)

    print('pdf')
    _pdf_total = 0
    _pdf_unique_total = 0
    for k, v in pdf_dict.items():
        print('未去重', k, len(v))
        print('去重', k, len(set(v)))
        _pdf_total += len(v)
        _pdf_unique_total += len(set(v))
    print('total', _pdf_total)
    print('total_unique', _pdf_unique_total)
    print('Hello World')
