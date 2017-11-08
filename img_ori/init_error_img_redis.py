#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/6 下午7:53
# @Author  : Hou Rong
# @Site    : 
# @File    : init_error_img_redis.py
# @Software: PyCharm
import redis
from data_source import MysqlSource
from logger import get_logger

logger = get_logger("init_error_img")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'ServicePlatform'
}
r = redis.Redis(host='10.10.180.145', db=1)

if __name__ == '__main__':
    query_sql = '''SELECT file_name
FROM error_f_md5_file_poi;'''

    _count = 0
    for line in MysqlSource(poi_ori_config, table_or_query=query_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=False):
        _count += 1
        if _count % 10000 == 0:
            logger.debug("[error img][count: {}]".format(_count))
        r.set('error_img_{}'.format(line[0]), '1')
    logger.debug("[total img][count: {}]".format(_count))
