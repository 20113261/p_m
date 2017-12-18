#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/15 下午4:25
# @Author  : Hou Rong
# @Site    : 
# @File    : data_diff.py
# @Software: PyCharm
import pandas
from urllib.parse import urlparse
from sqlalchemy.engine import create_engine
from service_platform_conn_pool import fetchall, service_platform_pool, poi_ori_pool
from toolbox.Common import is_legal
from collections import defaultdict
from logger import get_logger
from data_source import MysqlSource

logger = get_logger("crawl_diff_check")

no_check = ()
check_name = [
    ('name', True, False),
    ('name_en', True, False),
    ('map_info', True, False),
    ('opentime', True, False),

    ('address', False, False),
    ('plantocounts', False, True),
    ('beentocounts', False, True),
    ('ranking', False, True),
    ('grade', False, True),
    ('commentcounts', False, True),
    ('imgurl', False, False),
    ('introduction', False, False),
]

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'poi_merge'
}

service_platform_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'ServicePlatform'
}


def get_old_info_dict():
    sql = '''SELECT
  id,
  source,
  name,
  name_en,
  map_info,
  address,
  plantocounts,
  beentocounts,
  ranking,
  grade,
  commentcounts,
  imgurl,
  introduction,
  opentime
FROM poi_merge.attr 
WHERE source='qyer';'''
    __dict = defaultdict(dict)
    _count = 0
    for line in MysqlSource(poi_ori_config, table_or_query=sql,
                            size=5000, is_table=False,
                            is_dict_cursor=True):
        _count += 1
        if _count % 3000 == 0:
            logger.info("[load old data info][count: {}]".format(_count))
        sid = line['id']

        for key_name, is_strict, num_check in check_name:
            if is_strict:
                __dict[sid][key_name] = line[key_name]
            else:
                legal_res = is_legal(line[key_name])
                if not num_check:
                    check_res = legal_res
                else:
                    try:
                        if int(legal_res) in (-1, 0):
                            check_res = False
                        else:
                            check_res = True
                    except Exception:
                        check_res = False
                __dict[sid][key_name] = check_res
    logger.info("[load old data info finished][count: {}]".format(_count))
    return __dict


def get_new_info_dict():
    # 新增数据统计
    new_data_count = 0

    # 用于记录新的 sid ，判断多少 sid 的数据没有回来
    new_sid_set = set()

    # 严格数据的差异记录
    diff_dict = defaultdict(set)

    # 非严格数据的改善记录
    improve_dict = defaultdict(set)

    # 非严格数据的糟糕记录
    worse_dict = defaultdict(set)

    # 字段全正确的集合
    all_right_sids = set()

    old_info_dict = get_old_info_dict()
    sql = '''SELECT
      id,
      source,
      name,
      name_en,
      map_info,
      address,
      plantocounts,
      beentocounts,
      ranking,
      grade,
      commentcounts,
      imgurl,
      introduction,
      opentime
    FROM qyer_whole_world;'''
    _count = 0
    for line in MysqlSource(service_platform_config, table_or_query=sql,
                            size=5000, is_table=False,
                            is_dict_cursor=True):
        _count += 1
        if _count % 3000 == 0:
            logger.info("[load new data info][count: {}]".format(_count))
        sid = line['id']
        new_sid_set.add(sid)

        all_right = True
        for key_name, is_strict, num_check in check_name:
            legal_res = is_legal(line[key_name])
            if not num_check:
                if not legal_res:
                    all_right = False
            else:
                try:
                    if int(legal_res) in (-1, 0):
                        all_right = False
                except Exception:
                    all_right = False
        if all_right:
            all_right_sids.add(sid)

        if sid not in old_info_dict:
            new_data_count += 1
            continue
        else:
            old_info = old_info_dict[sid]

        for key_name, is_strict, num_check in check_name:
            if is_strict:
                if line[key_name] != old_info[key_name]:
                    # 严格字段差异统计
                    diff_dict[key_name].add(sid)
            else:
                # 非严格字段统计
                legal_res = is_legal(line[key_name])
                if not num_check:
                    check_res = legal_res
                else:
                    try:
                        if int(legal_res) in (-1, 0):
                            check_res = False
                        else:
                            check_res = True
                    except Exception:
                        check_res = False
                if check_res == old_info[key_name]:
                    continue
                elif check_res:
                    improve_dict[key_name].add(sid)
                else:
                    worse_dict[key_name].add(sid)
    logger.info("[load new data info finished][count: {}]".format(_count))

    return new_data_count, set(old_info_dict.keys()) - new_sid_set, diff_dict, improve_dict, worse_dict, all_right_sids


def img_unique(img_str):
    _set = set()
    for each in img_str.split('|'):
        url_obj = urlparse(each)
        _set.add(url_obj.path)
    return len(_set)


def generate_all_right_sample(all_rights):
    engine = create_engine('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/ServicePlatform?charset=utf8')
    table = pandas.read_sql('''SELECT
  id,
  source,
  name,
  name_en,
  map_info,
  address,
  plantocounts,
  beentocounts,
  ranking,
  grade,
  commentcounts,
  imgurl,
  url,
  introduction,
  opentime
FROM qyer_whole_world;''', engine)
    logger.info("[load table finished]")
    all_right_table = table[table['id'].isin(all_rights)].copy()
    all_right_table['img_num'] = all_right_table['imgurl'].apply(img_unique)
    logger.info("[all right table finished]")
    not_all_right_table = table[~table['id'].isin(all_rights)].copy()
    not_all_right_table['img_num'] = not_all_right_table['imgurl'].apply(img_unique)
    logger.info("[not all right table finished]")

    logger.info("[all right data][len: {}]".format(len(all_right_table)))
    logger.info("[not all right data][len: {}]".format(len(not_all_right_table)))

    all_right_table.sample(100).to_csv('/tmp/all_right_sample.csv')
    not_all_right_table.sample(100).to_csv('/tmp/not_all_right_sample.csv')


def report():
    new_data_count, lost_data, diff, improve, worse, all_rights = get_new_info_dict()
    generate_all_right_sample(all_rights)

    logger.info("[new data][count: {}]".format(new_data_count))
    logger.info("[lost data][count: {}][data: {}]".format(len(lost_data), lost_data))

    for k, v in diff.items():
        logger.info("[diff data][key: {}][count: {}][sid: {}]".format(k, len(v), v))

    for k, v in improve.items():
        logger.info("[improve data][key: {}][count: {}][sid: {}]".format(k, len(v), v))

    for k, v in worse.items():
        logger.info("[worse data][key: {}][count: {}][sid: {}]".format(k, len(v), v))


if __name__ == '__main__':
    report()
