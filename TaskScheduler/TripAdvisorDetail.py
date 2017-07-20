#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/14 上午10:06
# @Author  : Hou Rong
# @Site    : 
# @File    : TripAdvisorDetail.py
# @Software: PyCharm
import pymysql
import pymongo

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['HotelList']['TripAdvisor']
conn = pymysql.connect(host='10.10.228.253', user='writer', password='miaoji1109', db='spider_db', charset='utf8')


def insert_db(args):
    return cursor.executemany(
        '''REPLACE INTO hotel_base_data_task_by_serviceplatform (source, source_id, city_id, hotel_url) VALUES (%s, %s, %s, %s)''', args)


if __name__ == '__main__':
    cursor = conn.cursor()
    data = []
    _count = 0
    for line in collections.find():
        city_id = line['city_id']
        source_id = line['source_id']
        source_url = line['source_url']
        data.append(('tripadvisor', source_id, city_id, source_url))
        _count += 1

        if len(data) % 3000 == 0:
            print('Now', _count)
            print('Insert', insert_db(data))
            data = []

    print("Now", _count)
    print("Insert", insert_db(data))

    # id_set = set()
    # _count = 0
    # for line in collections.find():
    #     _count += 1
    #     id_set.add(line['sid'])
    #     if _count % 10000 == 0:
    #         print(_count)
    #
    # f = open('/root/data/task/ctrip_sid', 'w')
    # for sid in id_set:
    #     f.write("{}\n".format(sid))
    #
    # print('total length', len(id_set))
    # f.close()
