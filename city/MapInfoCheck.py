#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/5 上午11:14
# @Author  : Hou Rong
# @Site    : 
# @File    : MapInfoCheck.py
# @Software: PyCharm
import dataset
import re

if __name__ == '__main__':
    pattern = re.compile('-?\d+(\.-?\d+)?')
    check_keys = ['map_info', 'border_map_1', 'border_map_2']
    # db = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8')
    db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/base_data?charset=utf8')

    for key in check_keys:
        print('#' * 100)
        print(key)
        for line in db['city'].all():
            if line[key] in ('NULL', ''):
                continue
            try:
                lon, lat = line[key].split(',')
                if pattern.match(lon) is None:
                    raise Exception()
                if pattern.match(lat) is None:
                    raise Exception()

            except Exception:
                print(line['id'], line[key])

    print('#' * 100)
