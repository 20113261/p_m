#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/26 下午9:39
# @Author  : Hou Rong
# @Site    : 
# @File    : filter_data_already_online.py
# @Software: PyCharm
from service_platform_conn_pool import poi_ori_pool, base_data_pool
from logger import get_logger

logger = get_logger("filter_data_already_online")


def filter_data_already_online(_type, _mioji_id, error):
    if _type == 'attr':
        table_name = 'chat_attraction'
    elif _type == 'rest':
        table_name = 'chat_restaurant'
    elif _type == 'shop':
        table_name = 'chat_shopping'
    else:
        raise TypeError("Unknown Type: {}".format(_type))

    _status = None
    try:
        conn = base_data_pool.connection()
        cursor = conn.cursor()
        sql = '''SELECT status_test FROM {} WHERE id='{}';'''.format(table_name, _mioji_id)
        _res = cursor.execute(sql)
        if not _res:
            return False
        _status = cursor.fetchone()[0]
        logger.debug("[type: {}][status: {}]".format(_type, _status))
        cursor.close()
        conn.close()
    except Exception as exc:
        logger.exception(msg="[get online poi status error]", exc_info=exc)

    if not _status:
        return False

    try:
        conn = poi_ori_pool.connection()
        cursor = conn.cursor()
        sql = '''INSERT IGNORE INTO filter_data_already_online (type, mioji_id, error, status) VALUES (%s, %s, %s, %s);'''
        cursor.execute(sql, (_type, _mioji_id, error, _status))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as exc:
        logger.exception(msg="[insert filter data already online error]", exc_info=exc)


if __name__ == '__main__':
    filter_data_already_online("attr", "v200001", "test error")
