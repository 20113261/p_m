#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/21 下午9:31
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_google_task.py
# @Software: PyCharm
# from MongoTask.MongoTaskInsert import InsertTask

# f = open('/Users/hourong/Downloads/google_drive_url.txt')
# f = open('/search/hourong/task/target_url_1128')
# with InsertTask(worker='proj.total_tasks.crawl_json', queue='file_downloader', routine_key='file_downloader',
#                 task_name='google_drive_task_20171129', source='Google', _type='GoogleDriveTask',
#                 priority=11) as it:
#     for line in f:
#         g_url, flag, table_name = line.strip().split('###')
#         it.insert_task({
#             'url': g_url,
#             'flag': flag,
#             'table_name': table_name
#         })

from MongoTask.MongoTaskInsert import InsertTask
from service_platform_conn_pool import fetchall, base_data_pool
from my_logger import get_logger

logger = get_logger("generate_google_task")


def generate_dict():
    __country_dic = {}
    __city_dic = {}
    __map_dict = {}
    sql = '''SELECT *
FROM city;'''
    for line in fetchall(base_data_pool, sql, is_dict=True):
        __country_dic.setdefault(line['country_id'], [])
        __country_dic[line['country_id']].append(line)
        __city_dic['{0}'.format(line['id'])] = line
        __map_dict[line['id']] = line['map_info']
    return __country_dic, __city_dic, __map_dict


def is_map_info_legal(map_info):
    try:
        lon, lat = map_info.split(',')
        float(lon)
        float(lat)
        if lon == 0 and lat == 0:
            raise Exception()
        return True
    except Exception:
        print("[map info illegal][map_info: {}]".format(map_info))
        return False


def city_pair(city_ids):
    country_dict, city_dict, map_dict = generate_dict()
    pair = set([])
    pair_filter = []
    for c_id in city_ids:
        current_city = city_dict[c_id]
        all_city = country_dict[current_city['country_id']]
        for ac in all_city:
            if c_id == ac['id']:
                continue
            pf = '{0}-{1}'.format(c_id, ac['id'])
            if pf not in pair_filter:
                pair_filter.append(pf)
                src_cid = c_id
                dst_cid = ac['id']

                src_map_info = map_dict.get(src_cid)
                dst_map_info = map_dict.get(dst_cid)

                if not is_map_info_legal(src_map_info) or not is_map_info_legal(dst_map_info):
                    logger.warning("[error map info][src_cid: {}][dst_cid: {}][src_m_info: {}][dst_m_info: {}]".format(
                        src_cid,
                        dst_cid,
                        src_map_info,
                        dst_map_info
                    ))
                    continue

                google_url = 'http://maps.google.cn/maps/api/directions/json?origin={}&destination={}&mode=driving'.format(
                    src_map_info,
                    dst_map_info
                )
                logger.info("[new task][url: {}]".format(google_url))
                pair.add((
                    src_cid,
                    dst_cid,
                    google_url
                ))
    return pair


if __name__ == '__main__':
    cids = ['40050', '40051', '40052', '40053', '51516', '51517', '51518', '51519', '51520', '51521', '51522']

    res = city_pair(cids)

    with InsertTask(worker='proj.total_tasks.crawl_json', queue='file_downloader', routine_key='file_downloader',
                    task_name='google_drive_task_20171129', source='Google', _type='GoogleDriveTask',
                    priority=11) as it:
        for s_cid, d_cid, google_url in res:
            it.insert_task({
                'url': google_url,
                'flag': it.task_name,
                'table_name': 'new_crawled_html_602'
            })
