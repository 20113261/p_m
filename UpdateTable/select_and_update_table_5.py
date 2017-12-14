#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 下午9:34
# @Author  : Hou Rong
# @Site    : 
# @File    : select_and_update_table.py
# @Software: PyCharm
import re
import json
from data_source import MysqlSource
from service_platform_conn_pool import poi_ori_pool, poi_face_detect_pool, service_platform_pool, base_data_final_pool, \
    fetchall
from logger import get_logger

logger = get_logger("select_and_update_table")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'poi_merge'
}


def update_sql(data):
    sql = '''UPDATE chat_attraction
SET beentocount = %s, plantocount = %s, commentcount = %s
WHERE id = %s;'''
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    try:
        _res = cursor.execute(sql, data)
    except Exception:
        print(sql)
        raise Exception()
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[total: {}][execute: {}]".format(1, _res))


def generate_qyer_url_id():
    sql = '''SELECT
  url,
  commentcounts,
  beentocounts,
  plantocounts
FROM detail_total_qyer_20171209a;'''
    _dict = {}
    for url, bc, cc, pc in fetchall(service_platform_pool, sql):
        sid = re.findall('place.qyer.com/poi/([\s\S]+)/', url)[-1]
        _dict[sid] = (bc, cc, pc)
    return _dict


def get_task():
    g_dict = generate_qyer_url_id()
    sql = '''SELECT
  id,
  commentcount,
  beentocount,
  plantocount,
  json_extract(url, '$.qyer')
FROM chat_attraction
WHERE id IN
      (
      
      );'''
    data = []
    _count = 0
    _err_count = 0
    _e_id_set = set()
    for uid, b_c, p_c, c_c, q_url in MysqlSource(poi_ori_config, table_or_query=sql,
                                                 size=10000, is_table=False,
                                                 is_dict_cursor=False):
        _count += 1
        if not str(q_url).endswith('/'):
            q_url += '/'
        q_url_id = re.findall('http://place.qyer.com/poi/(\S+?)/', q_url)[-1]

        bc_d = json.loads(b_c)
        # if 'qyer' in bc_d:
        #     del bc_d['qyer']
        pc_d = json.loads(p_c)
        # if 'qyer' in pc_d:
        #     del pc_d['qyer']
        cc_d = json.loads(c_c)
        # del cc_d['qyer']

        res = g_dict.get(q_url_id)
        if res:
            bc, cc, pc = res
            bc_d['qyer'] = int(bc)
            cc_d['qyer'] = int(cc)
            pc_d['qyer'] = int(pc)
            print(uid, json.dumps(bc_d), json.dumps(pc_d), json.dumps(cc_d), q_url_id, q_url)
        else:
            _err_count += 1
            _e_id_set.add((uid, q_url))
            print()
            print('##' * 10)
            print(uid, json.dumps(bc_d), json.dumps(pc_d), json.dumps(cc_d), q_url_id, q_url)
            print('##' * 10)
            print()

        logger.info("[count: {}]".format(_count))
        logger.info("[err_count: {}]".format(_err_count))
        # update_sql((json.dumps(bc_d), json.dumps(pc_d), json.dumps(cc_d), uid))
        # if len(data) == 1000:
        #     logger.info("[count: {}]".format(_count))
        #         update_sql(data)
        #         data = []
        # update_sql(data)
    print(_e_id_set)


if __name__ == '__main__':
    get_task()
