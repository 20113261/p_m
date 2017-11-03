#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/3 下午6:45
# @Author  : Hou Rong
# @Site    : 
# @File    : url_merge_report.py
# @Software: PyCharm
import json
import re
from data_source import MysqlSource
from collections import defaultdict
from logger import get_logger

logger = get_logger("url_id_merge_report_logger")

poi_ori_config = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}


def report():
    query_sql = '''SELECT
  id,
  url
FROM chat_attraction;'''

    union_dict = defaultdict(set)
    _count = 0
    for line in MysqlSource(poi_ori_config, table_or_query=query_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=True):
        _count += 1

        if _count % 10000 == 0:
            logger.debug("[now count: {}]".format(_count))

        _id = line['id']
        _url = line['url']
        if not _url:
            continue
        urls = json.loads(_url)

        if 'qyer' in urls:
            try:
                _source = 'qyer'
                _sid = re.findall('place.qyer.com/poi/([\s\S]+)/', urls['qyer'])[0]
                union_dict[(_source, _sid)].add(_id)
            except Exception:
                pass

        if 'daodao' in urls:
            try:
                _source = 'daodao'
                _sid = re.findall('-d(\d+)', urls['daodao'])[0]
                union_dict[(_source, _sid)].add(_id)
            except Exception:
                pass
    _count = 0
    for k, v in union_dict.items():
        if len(v) > 1:
            _count += 1
            logger.info("[ source, sid : {} ][ can be merged uid : {} ]".format(k, v))
    logger.info("[total: {}]".format(_count))


if __name__ == '__main__':
    report()
