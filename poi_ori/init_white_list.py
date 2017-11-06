#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/6 下午12:35
# @Author  : Hou Rong
# @Site    : 
# @File    : init_white_list.py
# @Software: PyCharm
import re
import json
from pymysql.cursors import DictCursor
from service_platform_conn_pool import poi_ori_pool
from logger import get_logger
from toolbox.Hash import get_token
from collections import defaultdict

logger = get_logger("init_white_list")


def init_white_list():
    _dict = {}
    conn = poi_ori_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    query_sql = '''SELECT
  id,
  url
FROM attr
WHERE source = 'qyer';'''
    cursor.execute(query_sql)
    _count = 0
    for line in cursor.fetchall():
        _count += 1
        if _count % 2000 == 0:
            logger.debug("[init dict][count: {}]".format(_count))
        try:
            _url = line['url']
            _url_id = re.findall('place.qyer.com/poi/([\s\S]+)/', _url)[0]
            _sid = line['id']
            _dict[_url_id] = _sid
        except Exception as exc:
            logger.exception(msg="[init qyer id dict error]", exc_info=exc)
    cursor.close()
    conn.close()
    logger.debug("[init dict][count: {}]".format(_count))
    return _dict


def insert_white_list(data):
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany('''REPLACE INTO white_list (type, md5, info) VALUES (%s, %s, %s)''', data)
    conn.commit()
    logger.debug("[insert white list][total: {}][insert: {}]".format(len(data), _res))
    cursor.close()
    conn.close()


def main(_poi_type):
    _dict = init_white_list()
    import pandas
    table = pandas.read_csv('/tmp/qyer_to_daodao.csv').fillna('')

    _data = []
    for i in range(len(table)):
        line = table.iloc[i]
        qyer_url = line['穷游链接  http://www.qyer.com/']
        daodao_url = line['到到链接  https://www.tripadvisor.cn/']

        if not qyer_url:
            continue

        _url_id = re.findall('place.qyer.com/poi/([\s\S]+)/', qyer_url)[0]
        _qyer_sid = _dict[_url_id]
        _daodao_sid_pre = re.findall('-d(\d+)', daodao_url)

        if not _daodao_sid_pre:
            continue
        else:
            _daodao_sid = _daodao_sid_pre[0]

        info = {
            'qyer': _qyer_sid,
            'qyer_url_id': _url_id,
            'daodao': _daodao_sid
        }
        token = get_token(info)
        _data.append((_poi_type, token, json.dumps(info)))
        print(_qyer_sid, _daodao_sid)
    insert_white_list(data=_data)


if __name__ == '__main__':
    main('attr')
