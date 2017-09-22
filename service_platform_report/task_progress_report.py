#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/22 上午11:04
# @Author  : Hou Rong
# @Site    : 
# @File    : task_progress_report.py
# @Software: PyCharm
import redis
import datetime
import dataset
import json
from collections import defaultdict

if __name__ == '__main__':
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    product_table = db['serviceplatform_product_summary']
    product_error_table = db['serviceplatform_product_error_summary']

    r = redis.Redis(host='10.10.180.145', db=9)
    dt = datetime.datetime.now()
    product_count = defaultdict(int)
    product_error_count = defaultdict(int)

    for key in r.keys():
        key_list = key.decode().split('|_|')
        count = r.get(key)

        if len(key_list) == 4:
            task_tag, task_source, task_type, report_key = key_list
            product_count[(task_tag, task_source, task_type, report_key)] = int(count)
        elif len(key_list) == 5:
            task_tag, task_source, task_type, report_key, task_error_code = key_list
            product_error_count[(task_tag, task_source, task_type, report_key, task_error_code)] = int(count)
        else:
            continue

    # 更新产品统计
    for key, value in product_count.items():
        task_tag, task_source, task_type, report_key = key
        data = {
            'tag': task_tag,
            'source': task_source,
            'type': task_type,
            'report_key': report_key,
            'num': value,
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }

        try:
            product_table.upsert(data, keys=['tag', 'source', 'type', 'report_key', 'date'],
                                 ensure=None)
        except Exception:
            pass

        print(json.dumps(data, indent=4, sort_keys=True))

    # 更新产品错误统计
    for key, value in product_error_count.items():
        task_tag, task_source, task_type, report_key, product_error_count = key
        data = {
            'tag': task_tag,
            'source': task_source,
            'type': task_type,
            'error_code': product_error_count,
            'num': value,
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }

        try:
            product_error_table.upsert(data, keys=['tag', 'source', 'type', 'error_code', 'date'], ensure=None)
        except Exception:
            pass

        print(json.dumps(data, indent=4, sort_keys=True))
