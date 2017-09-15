#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 下午9:59
# @Author  : Hou Rong
# @Site    : 
# @File    : green_report.py
# @Software: PyCharm
import dataset
import datetime


def insert_report_data(_type, _total, _lost):
    db = dataset.connect('mysql+pymysql://writer:miaoji1109@10.10.228.253/Report?charset=utf8')
    table = db['img_lost_summary']

    dt = datetime.datetime.now()

    data = {
        'type': _type,
        'total': _total,
        'lost': _lost,
        'date': datetime.datetime.strftime(dt, '%Y%m%d'),
        'hour': datetime.datetime.strftime(dt, '%H'),
        'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
    }

    table.upsert(data, keys=['type', 'date'], ensure=None)

    print(_type, _total, _lost,
          datetime.datetime.strftime(dt, '%Y%m%d'),
          datetime.datetime.strftime(dt, '%H'), datetime.datetime.strftime(dt, '%Y%m%d%H00'))


if __name__ == '__main__':
    import sys

    args = sys.argv
    if len(args) != 4:
        print("Wrong Length, green_report.py type total lost")
        exit(137)
    else:
        insert_report_data(*args[1:])
        exit(0)
