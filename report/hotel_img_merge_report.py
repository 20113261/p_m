#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/7 上午11:28
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_img_merge_report.py
# @Software: PyCharm
import redis
import pandas
from collections import defaultdict

r = redis.Redis(host='10.10.180.145', db=2)


def report(target):
    img_all = defaultdict(int)
    img_finished = defaultdict(int)
    img_percentage = defaultdict(int)

    for key in r.keys():
        l_key = key.decode().split('|_|')
        if len(l_key) != 4:
            continue

        report_key, task_id, pixel_filter, source = l_key

        if pixel_filter != str(target):
            # 过滤不是当前需要的像素
            continue

        value = int(r.get(key).decode())
        if report_key == 'total':
            img_all['total'] += value
            img_all[source] += value
        elif report_key == 'finished':
            img_finished['total'] += value
            img_finished[source] += value
        elif report_key in ('0_10', '10_30', '30_max'):
            img_percentage['total'] += value
            img_percentage[report_key] += value

    data = []
    for k in sorted(img_all.keys()):
        data.append((k, img_all[k], img_finished[k]))

    table_source = pandas.DataFrame(columns=['source', 'total', 'finished'], data=data)
    table_source['failed_percent'] = ((table_source['total'] - table_source['finished']) / table_source[
        'total']) * 100

    table_img_percent = pandas.DataFrame(columns=[''])
    return table_source


table = report(200000)
table
