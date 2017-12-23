#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/19 下午4:30
# @Author  : Hou Rong
# @Site    : 
# @File    : error_img_info_scan.py
# @Software: PyCharm
from data_source import MysqlSource

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'BaseDataFinal'
}


def get_file_name():
    query_sql = '''SELECT source, source_id, pic_md5
FROM hotel_images
WHERE part = '20171127a' AND info IS NULL;'''
    for line in MysqlSource(poi_ori_config, table_or_query=query_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=False):
        yield line


f = open('/search/hourong/final_data/error_img', 'a')
for _id, (s, sid, f_name) in enumerate(get_file_name()):
    # print(_id, s, sid, f_name)
    if _id % 10000 == 0:
        print(_id)
    f.write('\t'.join([s, sid, f_name]) + '\n')
