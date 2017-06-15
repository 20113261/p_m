#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/15 下午3:52
# @Author  : Hou Rong
# @Site    : 
# @File    : DateRange.py
# @Software: PyCharm
import datetime

DATE_FORMAT = "%Y-%m-%d"


def date_range(start_date, end_date):
    dates = []
    dt = datetime.datetime.strptime(start_date, DATE_FORMAT)
    date = start_date[:]
    yield date
    while date < end_date:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime(DATE_FORMAT)
        yield date


def dates_until(end_date):
    dt = datetime.datetime.now()
    start_date = dt.strftime(DATE_FORMAT)
    yield from date_range(start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    for i in dates_until('2017-12-31'):
        print(i)
