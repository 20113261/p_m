#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/9 上午10:12
# @Author  : Hou Rong
# @Site    : 
# @File    : test_mysql_view.py
# @Software: PyCharm
import pymysql

conn = pymysql.connect(host='10.10.228.253', user='reader', password='mioji1109', charset='utf8', db='poi_merge')
cursor = conn.cursor()
cursor.execute('''SELECT new_id, old_id FROM old_id2new;''')
for line in cursor.fetchall():
    print(line)
cursor.close()
conn.close()
