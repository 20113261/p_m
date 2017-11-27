#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/27 下午8:52
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_new_task.py
# @Software: PyCharm
import copy
import json
import pymysql.cursors
from service_platform_conn_pool import source_info_pool


def task():
    conn = source_info_pool.connection()
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute('''SELECT
  city_id,
  source,
  suggest                AS suggestions,
  1                      AS select_index,
  100                    AS annotation,
  json_object('code', 0) AS error,
  label_batch,
  0                      AS is_new_type,
  sid
FROM tmp_ota_loc
WHERE suggest_type = 2;''')
    for line in cursor.fetchall():
        source = line['source']
        if source != 'ctrip':
            _n_line = copy.deepcopy(line)
            _n_line['suggestions'] = [eval(_n_line['suggestions'])]
        else:
            _n_line = copy.deepcopy(line)
            _n_line['suggestions'] = [_n_line['suggestions']]
        _n_line["suggestions"] = json.dumps(_n_line["suggestions"])
        cursor.execute(
            '''REPLACE INTO hotel_suggestions_city (city_id, source, suggestions, select_index, annotation, error, label_batch, is_new_type, sid) VALUES (%(city_id)s, %(source)s, %(suggestions)s, %(select_index)s, %(annotation)s, %(error)s, %(label_batch)s, %(is_new_type)s, %(sid)s)''',
            _n_line)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    task()
