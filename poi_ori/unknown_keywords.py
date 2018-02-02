#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/18 上午9:30
# @Author  : Hou Rong
# @Site    : 
# @File    : unknown_keywords.py
# @Software: PyCharm
from service_platform_conn_pool import poi_ori_pool
from toolbox.Hash import encode
from my_logger import func_time_logger, get_logger

logger = get_logger("insert unknown keywords")

count = 0


@func_time_logger
def insert_unknown_keywords(_type, _keyword_or_keywords):
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    sql = '''INSERT IGNORE INTO unknown_keywords (`type`, `key_hash`, `keywords`) VALUES (%s, %s, %s);'''
    if isinstance(_keyword_or_keywords, str):
        _hash_key = encode(_keyword_or_keywords)
        cursor.execute(sql, (_type, _hash_key, _keyword_or_keywords))
    elif isinstance(_keyword_or_keywords, (list, set, tuple)):
        for each_keyword in _keyword_or_keywords:
            _hash_key = encode(each_keyword)
            cursor.execute(sql, (_type, _hash_key, each_keyword))
    else:
        logger.debug("[unknown _keyword_or_keywords type: {}][_type: {}][_keyword_or_keywords: {}]".format(
            type(_keyword_or_keywords), _type, _keyword_or_keywords))
    conn.commit()
    cursor.close()
    conn.close()
