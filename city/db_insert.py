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
from city.config import base_path

def shareAirport_insert(config):
    db = dataset.connect('mysql+pymysql://{user}:{password}@{host}:3306/{db}?charset=utf8'.format(**config))
    airport_table = db['airport']
    table = pandas.read_csv(base_path+'share_airport.csv')

    _count = 0
    for i in range(len(table)):
        line = table.iloc[i]
        if line['airport_id'] != 'error':
            _count += 1
            data_line = airport_table.find_one(id=int(line['airport_id']))
            new_data = copy.deepcopy(data_line)
            new_data['city_id'] = int(line['city_id'])

            new_data.pop('id')
            new_data.pop('time2city_center')
            print('#' * 100)
            print(_count)
            print(new_data)

            airport_table.upsert(new_data, keys=['city_id', 'iata_code'])
    db.commit()
if __name__ == '__main__':
    shareAirport_insert()
