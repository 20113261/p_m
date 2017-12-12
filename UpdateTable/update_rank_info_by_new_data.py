#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/11 下午12:07
# @Author  : Hou Rong
# @Site    : 
# @File    : update_rank_info_by_new_data.py
# @Software: PyCharm
import json
from service_platform_conn_pool import service_platform_pool, poi_ori_pool


def update(data):
    sql = '''UPDATE chat_attraction
SET beentocount = %s, plantocount = %s
WHERE id = %s;'''
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    print("[total: {}][execute: {}]".format(len(data), _res))


def get_id():
    cross_dict = generate_cross_dict()
    _e_sid_set = set()
    _e_uid_set = set()
    sql = '''SELECT
  chat_attraction.id,
  chat_attraction.name,
  chat_attraction.name_en,
  attr_unid.source,
  attr_unid.source_id
FROM chat_attraction, attr_unid
WHERE beentocount = '{}' AND plantocount = '{}' AND data_source LIKE '%qyer%' AND attr_unid.source = 'qyer' AND
      chat_attraction.id = attr_unid.id;'''
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    data = []
    for uid, name, name_en, source, sid in cursor.fetchall():
        _tmp_res = cross_dict.get(sid)
        if _tmp_res:
            new_beentocounts, new_plantocounts = _tmp_res
            if new_beentocounts == '{}' and new_plantocounts == '{}':
                pass
            else:
                _e_uid_set.add(uid)
        else:
            print('[error sid: {}]'.format(sid))
            _e_sid_set.add(sid)
            continue
        data.append((new_beentocounts, new_plantocounts, uid))
        if len(data) == 1000:
            update(data=data)
            data = []
    if data:
        update(data=data)
    cursor.close()
    conn.close()
    print(_e_sid_set)
    print(_e_uid_set)


def generate_cross_dict():
    __dict = {}
    conn = service_platform_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT
  id,
  beentocounts,
  plantocounts
FROM detail_total_qyer_20171209a;''')
    for line in cursor.fetchall():
        if line[1]:
            beentocount = int(line[1])
        else:
            beentocount = -1
        if line[2]:
            plantocount = int(line[2])
        else:
            plantocount = -1

        if beentocount not in (-1, 0):
            beentocounts = json.dumps({'qyer': beentocount})
        else:
            beentocounts = '{}'

        if plantocount not in (-1, 0):
            plantocounts = json.dumps({'qyer': plantocount})
        else:
            plantocounts = '{}'

        __dict[line[0]] = (
            beentocounts,
            plantocounts
        )
    print("[cross dict finished]")
    return __dict


if __name__ == '__main__':
    # generate_cross_dict()
    get_id()
