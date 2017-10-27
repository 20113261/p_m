#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/27 上午11:59
# @Author  : Hou Rong
# @Site    : 
# @File    : merge_report.py
# @Software: PyCharm
import datetime
import json
import dataset
from service_platform_conn_pool import base_data_pool
from data_source import MysqlSource
from logger import get_logger
from collections import defaultdict
from toolbox.Common import is_legal, has_any

logger = get_logger("merge_report_old")
table_name = None
poi_name = None

# poi_ori_config = {
#     'host': '10.10.228.253',
#     'user': 'mioji_admin',
#     'passwd': 'mioji1109',
#     'db': 'poi_merge'
# }


data_process_config = {
    'host': '10.10.242.173',
    'user': 'root',
    'passwd': 'shizuo0907',
    'db': 'data_process'
}


def prepare_city_info():
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT
  id,
  CASE WHEN grade != -1
    THEN grade
  ELSE 100 END AS grade
FROM city;''')
    _res = {line[0]: line[1] for line in cursor.fetchall()}

    cursor.execute('''SELECT
  CASE WHEN grade = -1
    THEN 100
  ELSE grade END,
  count(*)
FROM city
GROUP BY grade;''')
    _grade_info = {line[0]: line[1] for line in cursor.fetchall()}
    cursor.close()
    conn.close()
    return _res, _grade_info


def poi_merged_report(poi_type):
    cid2grade, grade_info = prepare_city_info()
    if poi_type == 'attr':
        query_sql = '''SELECT
      id,
      city_id,
      first_image,
      address,
      open,
      introduction,
      data_source,
      status_online,
      utime
    FROM chat_attraction;'''
    elif poi_type == 'shop':
        query_sql = '''SELECT
              id,
              city_id,
              first_image,
              address,
              open,
              introduction,
              data_source,
              status_online,
              utime
            FROM chat_shopping;'''
    else:
        query_sql = '''SELECT
              id,
              city_id,
              first_image,
              address,
              open,
              introduction,
              data_source,
              status_online,
              utime
            FROM chat_restaurant;'''

    poi_info = defaultdict(dict)
    for line in MysqlSource(db_config=data_process_config, table_or_query=query_sql, size=10000, is_dict_cursor=True,
                            is_table=False):
        cid = line['city_id']

        # get grade
        grade = cid2grade.get(cid, None)

        if grade is None:
            # not known cid
            continue

        # add cid
        if 'has_poi' not in poi_info[grade]:
            poi_info[grade]['has_poi'] = set()
        poi_info[grade]['has_poi'].add(line['city_id'])

        # poi total
        if 'total' not in poi_info[grade]:
            poi_info[grade]['total'] = 0
        poi_info[grade]['total'] += 1

        # poi online
        if 'online' not in poi_info[grade]:
            poi_info[grade]['online'] = 0
        if 'Open' == line['status_online']:
            poi_info[grade]['online'] += 1

        # poi update this time
        if 'update' not in poi_info[grade]:
            poi_info[grade]['update'] = 0
        try:
            if line['utime'] > datetime.datetime.now() - datetime.timedelta(days=30):
                poi_info[grade]['update'] += 1
        except Exception as exc:
            logger.exception(msg="[unknown utime][utime: {}]".format(line['utime']), exc_info=exc)

        # poi has img
        if 'img' not in poi_info[grade]:
            poi_info[grade]['img'] = 0
        if is_legal(line['first_image']):
            poi_info[grade]['img'] += 1

        # poi has address
        if 'address' not in poi_info[grade]:
            poi_info[grade]['address'] = 0
        if is_legal(line['address']):
            poi_info[grade]['address'] += 1

        # poi opentime
        if 'opentime' not in poi_info[grade]:
            poi_info[grade]['opentime'] = 0
        if is_legal(line['open']):
            poi_info[grade]['opentime'] += 1

        # poi introduction
        if 'introduction' not in poi_info[grade]:
            poi_info[grade]['introduction'] = 0
        if is_legal(line['introduction']):
            try:
                _data = json.loads(line['introduction'])
                if isinstance(_data, dict):
                    if _data.values():
                        if has_any(list(_data.values()), check_func=is_legal):
                            poi_info[grade]['introduction'] += 1
            except Exception as exc:
                logger.exception(msg="[load introduction error][introduction: {}]".format(line['introduction']),
                                 exc_info=exc)

        # qyer\daodao\multi in source
        if 'qyer' not in poi_info[grade]:
            poi_info[grade]['qyer'] = 0
        if 'daodao' not in poi_info[grade]:
            poi_info[grade]['daodao'] = 0
        if 'multi' not in poi_info[grade]:
            poi_info[grade]['multi'] = 0

        if is_legal(line['data_source']):
            if 'qyer' in line['data_source']:
                poi_info[grade]['qyer'] += 1
            if 'daodao' in line['data_source'] or 'tripadvisor' in line['data_source']:
                poi_info[grade]['daodao'] += 1
            if 'qyer' in line['data_source'] and (
                            'daodao' in line['data_source'] or 'tripadvisor' in line['data_source']):
                poi_info[grade]['multi'] += 1

    for each_grade in poi_info.keys():
        poi_info[each_grade]['has_poi'] = len(poi_info[each_grade]['has_poi'])

    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    _table = db['base_data_report_summary']
    dt = datetime.datetime.now()
    for k, v in sorted(grade_info.items(), key=lambda x: x[0]):
        _tmp_grade_report = poi_info.get(k, defaultdict(int))
        no_poi = v - _tmp_grade_report['has_poi']
        data = {
            'type': poi_type,
            'grade': k,
            'citys': v,
            'no_poi': no_poi,
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }
        data.update(_tmp_grade_report)
        _table.upsert(data, keys=['type', 'grade', 'datetime'])
    db.commit()


if __name__ == '__main__':
    # import sys
    #
    # poi_type = sys.argv[1]
    # poi_merged_report(poi_type)
    poi_merged_report('attr')
    poi_merged_report('shop')
