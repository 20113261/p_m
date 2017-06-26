#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/26 下午2:36
# @Author  : Hou Rong
# @Site    : 
# @File    : PageInfo.py
# @Software: PyCharm
import pymongo
import datetime
import json
import pandas
from collections import defaultdict


def handling_func(_A, _B):
    json_1, json_2 = _A, _B
    compare_dict_1 = defaultdict(set)
    compare_dict_2 = defaultdict(set)
    for i in json_1['room']:
        name = i[0]
        guest_info = json.loads(i[-2])
        page_index = guest_info['page_index']
        compare_dict_1[page_index].add(name)

    for i in json_2['room']:
        name = i[0]
        guest_info = json.loads(i[-2])
        page_index = guest_info['page_index']
        compare_dict_2[page_index].add(name)

    total = 0
    right = 0

    compare_data = []
    for k in sorted(compare_dict_1.keys(), key=lambda x: int(x)):
        total += len(compare_dict_1[k])
        right += len(compare_dict_2[k] & compare_dict_1[k])

        if k in (0, 3, 10):
            compare_data.append(
                {
                    'now_page': k,
                    'right': str(right),
                    'total': str(total),
                    'percent': '{0:.2f}%'.format(right / total * 100)
                }
            )
    compare_data.append(
        {
            'now_page': k,
            'right': str(right),
            'total': str(total),
            'percent': '{0:.2f}%'.format(right / total * 100)
        }
    )

    return pandas.DataFrame(compare_data)


if __name__ == '__main__':
    client = pymongo.MongoClient(host='10.10.231.105', port=27017)
    collection = client['HotelInfoDict']['HotelInfoForReport']
    _count = 0

    source_list = ['agodaListHotel', 'bookingListHotel', 'expediaListHotel', 'hotelsListHotel']

    for source in source_list:
        log_file = open('/tmp/HotelReport/{}'.format(source), 'w')
        print('#' * 100, file=log_file)
        A, B = None, None
        for each in collection.find({'source': source}).sort('time', 1).sort('source'):
            if A is None:
                A = each
                continue

            if A['time'] - each['time'] <= datetime.timedelta(minutes=20):
                B = each
            else:
                A = each
                continue

            print('首次请求', B['time'], '间隔时间', A['time'] - B['time'], file=log_file)
            print(handling_func(A['data'], B['data']), file=log_file)
            print('#' * 100, file=log_file)
            # 执行完毕后归位
            A, B = None, None

        log_file.close()
