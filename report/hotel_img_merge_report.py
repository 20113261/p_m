#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/7 上午11:28
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_img_merge_report.py
# @Software: PyCharm
import redis
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

        print(report_key, task_id, source, value)

    print("hotel img num report")
    for k in sorted(img_all.keys()):
        print(k, img_all[k], img_finished[k])

    print("hotel img final report")


if __name__ == '__main__':
    report(200000)
