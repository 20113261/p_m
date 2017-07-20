#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/15 下午8:47
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_mongo_task.py
# @Software: PyCharm
import pymysql
import pymongo
import datetime

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['Task']['NewTask']

if __name__ == '__main__':
    f = open('/root/data/task/WholeSiteTask')
    f.readline()
    collections.remove()

    data = []
    count = 0
    for each_line in f:
        line = each_line.split('\t')
        if len(line) != 3:
            continue
        count += 1
        try:
            data.append({
                'args': {
                    'mid': line[0].strip(),
                    'type': line[1].strip(),
                    'source_url': line[2].strip()
                },
                'used_times': 0,
                'finished': 0,
                'utime': datetime.datetime.now()
            })
        except Exception:
            print(line)

        if count % 100000 == 0:
            print(count)
            collections.insert_many(data)
            data = []

    print(count)
    collections.insert_many(data)
