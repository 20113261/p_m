#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/7 下午6:46
# @Author  : Hou Rong
# @Site    : 
# @File    : update_qyer_city_id.py
# @Software: PyCharm
import Common.MiojiSimilarCityDict
import logging
from pymysql.cursors import DictCursor
from service_platform_conn_pool import source_info_pool
from Common.MiojiSimilarCityDict import MiojiSimilarCityDict
from my_logger import get_logger

logger = get_logger("update_qyer_city_id")
logger.setLevel(logging.INFO)


def update_city(source, sid, cid):
    conn = source_info_pool.connection()
    cursor = conn.cursor()
    query_sql = '''UPDATE ota_location
SET city_id = %s
WHERE source = %s AND sid = %s;'''
    cursor.execute(query_sql, (cid, source, sid))
    logger.debug("[update][source: {}][sid: {}][cid: {}]".format(source, sid, cid))
    cursor.close()
    conn.close()


def task():
    Common.MiojiSimilarCityDict.COUNTRY_KEYS.append('country_id')
    d = MiojiSimilarCityDict()

    conn = source_info_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute('''SELECT
  source,
  sid,
  suggest,
  country_id,
  s_city
FROM ota_location
WHERE source = 'qyer' AND city_id = 'NULL';''')
    for line in cursor.fetchall():
        matched_city = d.get_mioji_city_id((line['country_id'], line['s_city'].lower()))
        if not matched_city:
            continue
        elif len(matched_city) > 1:
            logger.info(
                "[country_id: {}][city: {}][url: {}][matched_keys: {}]".format(line['country_id'], line['s_city'],
                                                                               line['suggest'], matched_city))
        else:
            update_city(line['source'], line['sid'], list(matched_city)[0])

    cursor.close()
    conn.close()


if __name__ == '__main__':
    task()
