#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/23 下午2:18
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_crawl_img_info.py
# @Software: PyCharm
import pymysql
import time
import redis
from collections import defaultdict

if __name__ == '__main__':
    r = redis.Redis(host='10.10.114.35', db=8)
    # 酒店
    print('开始请求')
    print('{}'.format(time.time()))
    s_sid = defaultdict(int)
    _count = 0
    conn = pymysql.connect(host='10.10.189.213', user='hourong', password='hourong', charset='utf8', db='update_img')

    with conn as cursor:
        cursor.execute('''SELECT
      source,
      source_id,
      count(*) AS num
    FROM pic_relation
    GROUP BY source, source_id;''')
        for source, sid, num in iter(lambda: cursor.fetchone(), None):
            _count += 1
            if _count % 10000 == 0:
                print('pic_relation', _count)
            s_sid[(source, sid)] = num
        print('pic_relation', _count)
        cursor.close()

    _count = 0
    with conn as cursor:
        cursor.execute('''SELECT
  source,
  source_id,
  count(*) AS num
FROM pic_relation_0905
GROUP BY source, source_id;''')
        for source, sid, num in iter(lambda: cursor.fetchone(), None):
            _count += 1
            if _count % 10000 == 0:
                print('pic_relation_0905', _count)
            s_sid[(source, sid)] = max(s_sid[(source, sid)], num)
        print('pic_relation_0905', _count)
        cursor.close()

    _count = 0
    for k, v in s_sid.items():
        _count += 1
        if _count % 10000 == 0:
            print('insert', _count)
        r.set('|_|'.join(k), v)
    print('insert', _count)

    # POI
    s_sid = defaultdict(int)
    _count = 0
    with conn as cursor:
        cursor.execute('''SELECT
  source,
  sid,
  count(*) AS num
FROM poi_bucket_relation
GROUP BY source, sid;''')
        for source, sid, num in iter(lambda: cursor.fetchone(), None):
            _count += 1
            if _count % 10000 == 0:
                print('poi_bucket_relation', _count)
            s_sid[(source, sid)] = max(s_sid[(source, sid)], num)
        print('poi_bucket_relation', _count)
        cursor.close()
    conn.close()

    _count = 0
    for k, v in s_sid.items():
        _count += 1
        if _count % 10000 == 0:
            print('insert', _count)
        r.set('|_|'.join(k), v)
    print('insert', _count)
