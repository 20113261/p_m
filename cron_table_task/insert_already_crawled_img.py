#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/23 下午7:29
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_already_crawled_img.py
# @Software: PyCharm
import pymysql
import redis
import datetime

if __name__ == '__main__':
    print('start', datetime.datetime.now())
    conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', password='mioji1109', charset='utf8',
                           db='ServicePlatform')
    r = redis.Redis(host='10.10.114.35', db=9)
    _count = 0
    with conn as cursor:
        cursor.execute('SELECT md5 FROM crawled_url;')
        for line in iter(lambda: cursor.fetchone(), None):
            _count += 1
            if _count % 10000 == 0:
                print('Now', _count)
            r.set('{0}'.format(line[0]), 1)
        cursor.close()
    print("Final", _count)
