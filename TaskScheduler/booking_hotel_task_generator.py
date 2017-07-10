#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/7 下午12:07
# @Author  : Hou Rong
# @Site    : 
# @File    : booking_hotel_task_generator.py
# @Software: PyCharm
from TaskScheduler.TaskInsert import InsertTask


def generate_booking_task():
    url_set = set()
    s_sid_set = set()

    f = open('/root/data/task/s_sid_booking')

    _count = 0
    for l in f:
        line = l.strip().split('\t')

        _count += 1
        hotel_url = line[2].split('?')[0]
        if hotel_url.startswith('https://www.booking.com') or hotel_url.startswith('http://www.booking.com'):
            url_set.add(hotel_url)
            if (line[0], line[1]) not in s_sid_set:
                s_sid_set.add((line[0], line[1]))

                other_info = {
                    'source_id': line[1],
                    'city_id': 'NULL'
                }

                args = {'source': line[0], 'hotel_url': hotel_url, 'other_info': other_info,
                        'part': 'hotel_base_data_booking_new'}
                yield args
        if _count % 100000 == 0:
            print(_count)

    print(len(url_set))
    print(len(s_sid_set))


if __name__ == '__main__':
    task_name = 'hotel_base_data_booking_new'

    with InsertTask(worker='hotel_base_data', task_name=task_name) as it:
        for args in generate_booking_task():
            it.insert_task(args=args)
