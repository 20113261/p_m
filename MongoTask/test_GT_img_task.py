#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/8 上午8:19
# @Author  : Hou Rong
# @Site    :
# @File    : mongo_task.py
# @Software: PyCharm
from data_source import MysqlSource
from my_logger import get_logger
from MongoTask.MongoTaskInsert import InsertTask
import pymongo
from collections import defaultdict

logger = get_logger("insert_mongo_task")


# def get_tasks():
#     f = open('/tmp/img_list(7).csv')
#     for line in f:
#         sid, _, url = line.strip().split(',')
#         yield sid, url
def get_tasks():
    # sql = '''SELECT sid, url FROM img_list;'''
    # for line in fetchall(spider_base_tmp_wanle_pool, sql, is_dict=True):
    #     yield line['sid'], line['url']
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    db = client['data_result']
    se = defaultdict(set)
    for co in collections.find({}):
        pid = co['args']['pid_3rd']
        for c in co['result']:
            pro = len(se[pid])
            se[pid].add(c['first_image'])
            if pro < len(se[pid]):
                yield pid, c['first_image']

            for i in c['image_list']:
                pro = len(se[pid])
                se[pid].add(i)
                if pro < len(se[pid]):
                    yield pid, i

            for i in c['route_day']:
                for ii in i['detail']:
                    for iii in ii['image_list']:
                        pro = len(se[pid])
                        se[pid].add(iii)
                        if pro < len(se[pid]):
                            yield pid, iii

            for i in c['hotel']['plans']:
                pro = len(se[pid])
                se[pid].add(i['img'])
                if pro < len(se[pid]):
                    yield pid, i['img']



def insert_task():
    with InsertTask(worker='proj.total_tasks.images_task', queue='file_downloader', routine_key='file_downloader',
                    task_name='image_GT_ctrip', source='ctripGT', _type='FileDownloader',
                    priority=11) as it:
        for sid, url in get_tasks():
            args = {
                'source': "ctripGT",
                'source_id': sid,
                'target_url': url,
                'bucket_name': 'ctrip-grouptravel',
                'file_prefix': 'ctripGT',
                'is_poi_task': True,
                'need_insert_db': True,
            }
            it.insert_task(args)


if __name__ == '__main__':
    insert_task()
