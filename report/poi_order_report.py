#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/3 下午2:15
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_order_report.py
# @Software: PyCharm
import copy
import json
from pymysql.cursors import DictCursor
from service_platform_conn_pool import data_process_pool, poi_ori_pool


def get_qyer_grade_ranking():
    _dict = {}
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    query_sql = '''SELECT
  attr_unid.id,
  attr.grade,
  attr.ranking
FROM attr
  JOIN attr_unid ON attr_unid.source = attr.source AND attr_unid.source_id = attr.id
WHERE attr.source = 'qyer';'''
    cursor.execute(query_sql)
    for line in cursor.fetchall():
        _dict[line[0]] = (line[1] if line[1] else -1.0, line[2] if line[2] else -1)
    cursor.close()
    conn.close()
    return _dict


def get_report():
    data_dict = get_qyer_grade_ranking()
    conn = data_process_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    query_sql = '''SELECT
  chat_attraction.id,
  city.id    AS city_id,
  chat_attraction.name,
  chat_attraction.name_en,
  chat_attraction.ori_grade,
  city.grade AS city_grade,
  url
FROM chat_attraction
  JOIN tmp.city ON chat_attraction.city_id = tmp.city.id
WHERE data_source = 'qyer' AND city.grade != -1 AND city.status_test = 'Open' AND chat_attraction.status_test = 'Open';'''
    cursor.execute(query_sql)
    data = []
    for line in cursor.fetchall():
        each = copy.deepcopy(line)
        grade, ranking = data_dict.get(line['id'], (-1.0, -1))
        each['qyer_grade'] = float(grade)
        each['qyer_ranking'] = int(ranking)
        each['url'] = json.loads(line['url'])['qyer']
        # each['qyer_grade'] = float(json.loads(each['ori_grade']).get('qyer', -1.0))
        data.append(each)
    cursor.close()
    conn.close()

    f = open('/tmp/cases.csv', 'w')
    import csv

    writer = csv.writer(f)
    writer.writerow(['id', 'city_grade', 'qyer_grade', 'qyer_ranking', 'name', 'name_en', 'qyer_url', 'daodao_url'])
    for line in sorted(data, key=lambda x: (int(x['city_grade']), -x['qyer_grade'], x['qyer_ranking'])):
        print(line['id'], line['city_grade'], line['qyer_grade'], line['qyer_ranking'], line['name'],
              line['name_en'], line['url'])

        writer.writerow(
            [line['id'], line['city_grade'], line['qyer_grade'], line['qyer_ranking'], line['name'], line['name_en'],
             line['url']])


if __name__ == '__main__':
    get_report()
