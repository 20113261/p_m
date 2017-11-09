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


def report(target, task_filter):
    img_all = defaultdict(int)
    img_finished = defaultdict(int)
    img_percentage = defaultdict(int)

    for key in r.keys():
        l_key = key.decode().split('|_|')
        if len(l_key) != 4:
            continue

        report_key, task_id, pixel_filter, source = l_key

        if task_id != task_filter:
            continue

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
        elif report_key in ('0', '1_10', '11_30', '30_max'):
            img_percentage['total'] += value
            img_percentage[report_key] += value
        elif report_key == 'all_failed':
            img_percentage['all_failed'] += value

        print(report_key, task_id, source, value)

    print("hotel img num report")
    data = []
    for k in sorted(img_all.keys()):
        data.append((k, img_all[k], img_finished[k]))

    _table_source = pandas.DataFrame(columns=['source', 'total', 'finished'], data=data)
    _table_source['failed_percent'] = ((_table_source['total'] - _table_source['finished']) / _table_source[
        'total']) * 100

    # todo 之后删除，临时添加的
    img_percentage['all_failed'] = 200721

    _table_img_percentage = pandas.DataFrame(
        columns=['total', '0', 'all_failed (保留最好一张)', '1-10', '11-30', '>30'],
        data=[

            [img_percentage['total'], img_percentage['0'],
             img_percentage['all_failed'], img_percentage['1_10'],
             img_percentage['11_30'], img_percentage['30_max']]]
    )

    _table_img_percentage['all_failed (保留最好一张)'] = (
                                                       _table_img_percentage['all_failed (保留最好一张)'] /
                                                       _table_img_percentage[
                                                           'total']) * 100
    _table_img_percentage['0'] = (_table_img_percentage['0'] / _table_img_percentage['total']) * 100
    _table_img_percentage['1-10'] = (_table_img_percentage['1-10'] / _table_img_percentage['total']) * 100
    _table_img_percentage['11-30'] = (_table_img_percentage['11-30'] / _table_img_percentage['total']) * 100
    _table_img_percentage['>30'] = (_table_img_percentage['>30'] / _table_img_percentage['total']) * 100

    return _table_source, _table_img_percentage


table_source, table_img_percentage = report(200000, task_filter='merge_hotel_image_20171108_20')

print(table_source)
print()
print(table_img_percentage)
