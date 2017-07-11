#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/15 下午3:52
# @Author  : Hou Rong
# @Site    : 
# @File    : DateRange.py
# @Software: PyCharm
import datetime

DATE_FORMAT = "%Y-%m-%d"


def date_range(start_date, end_date, day_step=1, ignore_days=0):
    dates = []
    dt = datetime.datetime.strptime(start_date, DATE_FORMAT)
    dt += datetime.timedelta(ignore_days)
    date = dt.strftime(DATE_FORMAT)
    yield date
    while date < end_date:
        dates.append(date)
        dt += datetime.timedelta(day_step)
        date = dt.strftime(DATE_FORMAT)
        yield date


def dates_until(end_date, day_step=1, ignore_days=0):
    dt = datetime.datetime.now()
    start_date = dt.strftime(DATE_FORMAT)
    yield from date_range(start_date=start_date, end_date=end_date, day_step=day_step, ignore_days=ignore_days)


def dates_tasks(take_days, day_step=1, ignore_days=0):
    dt = datetime.datetime.now()
    for i in range(ignore_days, take_days + ignore_days, day_step):
        yield (dt + datetime.timedelta(i)).strftime(DATE_FORMAT)


if __name__ == '__main__':
    # for i in dates_until('2017-12-31'):
    #     print(i)
    for i in dates_tasks(90, 10, 20):
        print(i)
