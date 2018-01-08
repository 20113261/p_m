#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/31 下午3:16
# @Author  : Hou Rong
# @Site    : 
# @File    : coverage_report.py
# @Software: PyCharm
from data_source import MysqlSource
from service_platform_conn_pool import base_data_config, service_platform_config
from logger import get_logger

logger = get_logger("hotel_sid_coverage_report")


def hotel_unid_sid_set(source):
    query_sql = '''SELECT sid
FROM hotel_unid
WHERE source = '{}';'''.format(source)
    _set = set()
    count = 0
    for line in MysqlSource(base_data_config, table_or_query=query_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=False):
        count += 1
        if count % 10000 == 0:
            logger.info("[prepare unid sid][source: {}][count: {}]".format(source, count))
        _set.add(line[0])
    logger.info("[prepare unid sid][source: {}][count: {}]".format(source, count))
    return _set


def hotel_detail_sid_set(source, tag):
    table_name = 'detail_hotel_{}_{}'.format(source, tag)
    query_sql = '''SELECT source_id FROM {};'''.format(table_name)
    _set = set()
    count = 0
    for line in MysqlSource(service_platform_config, table_or_query=query_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=False):
        count += 1
        if count % 10000 == 0:
            logger.info("[prepare detail sid][table_name: {}][count: {}]".format(table_name, count))
        _set.add(line[0])
    logger.info("[prepare detail sid][table_name: {}][count: {}]".format(table_name, count))
    return _set


def check(source, tag):
    unid_sid = hotel_unid_sid_set(source)
    detail_sid = hotel_detail_sid_set(source, tag)

    logger.info("[unid sid][total: {}]".format(len(unid_sid)))
    logger.info("[detail sid][total: {}]".format(len(detail_sid)))

    lost_sid = unid_sid - detail_sid
    new_sid = detail_sid - unid_sid

    lost_file = open('/tmp/lost_sid_{}_{}.txt'.format(source, tag), 'w')
    lost_file.write("\n".join(map(lambda x: str(x), lost_sid)))
    logger.info("[lost sid][count: {}]".format(len(lost_sid), lost_sid))
    logger.info("[new sid][count: {}]".format(len(new_sid)))


if __name__ == '__main__':
    # source_list = ['ctrip', 'booking', 'hotels', 'agoda', 'expedia', 'elong']
    source_list = ['expedia']
    for source in source_list:
        check(source, '20171127a')

        # check('elong', '20171219a')
