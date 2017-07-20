#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/14 上午11:21
# @Author  : Hou Rong
# @Site    : 
# @File    : dump_result.py
# @Software: PyCharm
import pymongo
import pymysql
from Common.GetMd5 import encode_md5

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['FullSiteSpider']['AttrFullSiteNew']


def insert_db(args):
    conn = pymysql.connect(host='10.10.228.253', user='writer', password='miaoji1109', db='WholeSiteCrawl',
                           charset='utf8')
    cursor = conn.cursor()
    res = cursor.executemany(
        '''INSERT IGNORE INTO attraction (mid, type, url_md5, source_url) VALUES (%s, %s, %s, %s)''',
        args)
    cursor.close()
    conn.commit()
    conn.close()
    return res


if __name__ == '__main__':
    data = []
    _count = 0
    for line in collections.find():
        mid = line['parent_info']['id']
        for img in (line['img_url'] or []):
            _count += 1
            if img:
                data.append((mid, 'img', encode_md5(img), img))
                if len(data) % 10000 == 0:
                    print('New', _count)
                    print('Insert', insert_db(data))
                    data = []

        for pdf in (line['pdf_url'] or []):
            _count += 1
            data.append((mid, 'pdf', encode_md5(pdf), pdf))
            if pdf:
                if len(data) % 10000 == 0:
                    print('New', _count)
                    print('Insert', insert_db(data))
                    data = []

    print('New', _count)
    print('Insert', insert_db(data))
    data = []
