#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/20 下午3:18
# @Author  : Hou Rong
# @Site    : 
# @File    : new_station_id.py
# @Software: PyCharm
import dataset

if __name__ == '__main__':
    db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/onlinedb?charset=utf8')
    table = db['station']

    station_id_set = set()
    new_station_id_set = set()
    for line in db.query('''SELECT *
FROM station
ORDER BY id'''):
        if line['station_id'] not in station_id_set:
            station_id_set.add(line['station_id'])
        else:
            new_station_id_set.add(line['id'])

    for i in new_station_id_set:
        new_max_id = 'stt' + str(int(max(station_id_set)[3:]) + 1)
        station_id_set.add(new_max_id)
        print(i, new_max_id)
        print(
            table.update(
                {
                    'id': i,
                    'station_id': new_max_id,
                }, keys=['id']
            )
        )
