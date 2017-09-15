#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 下午7:44
# @Author  : Hou Rong
# @Site    : 
# @File    : db_insert.py
# @Software: PyCharm
import pandas
import dataset
import copy

if __name__ == '__main__':
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253:3306/base_data?charset=utf8')
    airport_table = db['airport']
    table = pandas.read_csv('/Users/hourong/Downloads/airport(1).csv')

    _count = 0
    for i in range(len(table)):
        line = table.iloc[i]
        if line['机场id'] != 'error':
            _count += 1
            data_line = airport_table.find_one(id=line['机场id'])
            new_data = copy.deepcopy(data_line)
            new_data['city_id'] = int(line['cid'])

            new_data.pop('id')
            print('#' * 100)
            print(_count)
            print(new_data)

            airport_table.insert(new_data)
