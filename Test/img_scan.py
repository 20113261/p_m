#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午9:21
# @Author  : Hou Rong
# @Site    : 
# @File    : img_scan.py
# @Software: PyCharm
from data_source import MysqlSource

poi_ori_config = {
    'host': '10.10.69.170',
    # 'host': '10.10.228.253',
    # 'user': 'mioji_admin',
    'user': 'reader',
    # 'password': 'mioji1109',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

sql = '''SELECT
  id,
  image_list
FROM chat_shopping;'''

_count = 0
for line in MysqlSource(poi_ori_config, table_or_query=sql,
                        size=10000, is_table=False,
                        is_dict_cursor=True):
    img_list = line['image_list']
    error = False
    _count += 1
    if _count % 10000 == 0:
        print(_count)
    if not img_list:
        continue
    for each in img_list.split('|'):
        if len(each) in (1, 2, 3):
            error = True
            break
    if error:
        print(line['id'], line['image_list'])
