#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/1/26 下午5:06
# @Author  : Hou Rong
# @Site    : 
# @File    : inter_city_task.py
# @Software: PyCharm
from datetime import datetime
from MongoTask.MongoTaskInsert import InsertTask
from service_platform_conn_pool import fetchall, base_data_pool,init_pool
from my_logger import get_logger
from copy import deepcopy
from toolbox.Common import is_legal
logger = get_logger("generate_google_task")


def generate_dict(config):
    __country_dic = {}
    __city_dic = {}
    __map_dict = {}
    sql = '''SELECT *
FROM city;'''
    temp_config = deepcopy(config)
    temp_config['database'] = temp_config['db']
    del temp_config['db']
    del temp_config['charset']
    data_pool = init_pool(**temp_config)
    for line in fetchall(data_pool, sql, is_dict=True):
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


def city_pair(city_ids,config):
    country_dict, city_dict, map_dict = generate_dict(config)
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
                if not is_legal(dst_map_info):continue
                logger.info('%s: %s  - %s: %s' % (src_cid, src_map_info, dst_cid, dst_map_info))
                src_map_info_list = src_map_info.split(',')
                src_map_info = ','.join([src_map_info_list[1],src_map_info_list[0]])
                dst_map_info_list = dst_map_info.split(',')
                dst_map_info = ','.join([dst_map_info_list[1],dst_map_info_list[0]])
                if not is_map_info_legal(src_map_info) or not is_map_info_legal(dst_map_info):
                    logger.warning("[error map info][src_cid: {}][dst_cid: {}][src_m_info: {}][dst_m_info: {}]".format(
                        src_cid,
                        dst_cid,
                        src_map_info,
                        dst_map_info
                    ))
                    continue

                google_url = 'http://maps.google.cn/maps/api/directions/json?origin={}&destination={}&mode=driving&region=es&mode=driving&type=interCitya1={}&a2={}'.format(

                    src_map_info,
                    dst_map_info,
                    src_cid,
                    dst_cid
                )


                logger.info("[new task][url: {}]".format(google_url))
                pair.add((
                    src_cid,
                    dst_cid,
                    google_url
                ))
    return pair

def google_driver(city_id,param,config,):
    res = city_pair(city_id,config)
    # todo 需要修改 task_name (最好按照工单 id 生成)，添加特殊标记，例如 inter city，别和城市内重复
    time_tag = str(datetime.now())[:10].replace('-','')

    task_name = 'google_drive_{0}_{1}'.format(param,time_tag)


    with InsertTask(worker='proj.total_tasks.google_drive_task', queue='file_downloader', routine_key='file_downloader',
                    task_name=task_name, source='Google', _type='GoogleDriveTask',
                    priority=11) as it:
        for s_cid, d_cid, google_url in res:
            it.insert_task({
                'url': google_url,

                'task_id': 'inter_{0}'.format(param),
                's_cid': s_cid,
                'd_cid': d_cid

            })
            pass
        return it.generate_collection_name(),task_name


def city_inter_google_driver(urls,param):
    time_tag = str(datetime.now())[:10].replace('-', '')
    task_name = 'google_drivein_{0}_{1}'.format(param, time_tag)

    with InsertTask(worker='proj.total_tasks.google_drive_task', queue='file_downloader', routine_key='file_downloader',
                    task_name=task_name, source='Google', _type='GoogleDriveTask',
                    priority=11) as it:
        for google_url in urls:
            it.insert_task({
                'url': google_url,
                'task_id': 'inner_{0}'.format(param)
            })
        return it.generate_collection_name(),task_name

if __name__ == '__main__':
    cids = ['40050', '40051', '40052', '40053', '51516', '51517', '51518', '51519', '51520', '51521', '51522']
    print(str(datetime.now())[:10])

