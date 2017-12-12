#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/12 下午12:03
# @Author  : Hou Rong
# @Site    : 
# @File    : ImgErrorMd5Search.py
# @Software: PyCharm
import random
from data_source import MysqlSource
from service_platform_conn_pool import base_data_pool, fetchall
from logger import get_logger
from collections import defaultdict

logger = get_logger("img_error_md5_search")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'ServicePlatform'
}


def used_file_name():
    sql = '''SELECT
  first_image,
  image_list
FROM chat_shopping;'''
    _img_set = set()
    _count = 0
    for first_img, img_list in fetchall(base_data_pool, sql):
        _count += 1
        if _count % 5000 == 0:
            logger.info("[file_name_detected][count: {}]".format(_count))
        if not str(first_img).startswith('sh'):
            _img_set.add(first_img)
        for img_name in img_list.split('|'):
            if not str(img_name).startswith('sh'):
                _img_set.add(img_name)
    logger.info("[file_name_detected][count: {}]".format(_count))
    return _img_set


# def get_error_file_name():
#     error_img_set = used_file_name()
#     e_dict = defaultdict(set)
#     query_sql = '''SELECT file_name
# FROM error_f_md5_file_poi;'''
#     for line in MysqlSource(poi_ori_config, table_or_query=query_sql,
#                             size=10000, is_table=False,
#                             is_dict_cursor=False):
#         file_name = line[0]
#         if file_name in error_img_set:
#             if file_name:
#                 logger.warning("[error_img_name][file_name: {}]".format(file_name))
#                 _, ext_name = file_name.split('.')
#                 e_dict[ext_name].add(file_name)
#
#     for k, v in e_dict.items():
#         tmp = list(v)
#         random.shuffle(tmp)
#         print(k, tmp[:10])


if __name__ == '__main__':
    # get_error_file_name()
    s = used_file_name()
    print(len(s))
