#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/17 下午8:55
# @Author  : Hou Rong
# @Site    : 
# @File    : un_crawled_website_url.py
# @Software: PyCharm
import pymysql

if __name__ == '__main__':
    conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', password='mioji1109', db='hotel_adding',
                           charset='utf8')
    cursor = conn.cursor()
    cursor.execute('''SELECT DISTINCT
  source_id,
  description
FROM hotelinfo_tripadvisor
WHERE description != 'NULL';''')