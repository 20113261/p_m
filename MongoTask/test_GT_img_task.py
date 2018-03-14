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
import pymysql
from DBUtils.PooledDB import PooledDB
import gevent
from gevent import pool
from collections import defaultdict
import re
from MongoTask.MongoTaskInsert import InsertTask, TaskType
logger = get_logger("insert_mongo_task")
execute_pool = pool.Pool(2000)

# def get_tasks():
#     f = open('/tmp/img_list(7).csv')
#     for line in f:
#         sid, _, url = line.strip().split(',')
#         yield sid, url
def get_GT_tasks():
    # sql = '''SELECT sid, url FROM img_list;'''
    # for line in fetchall(spider_base_tmp_wanle_pool, sql, is_dict=True):
    #     yield line['sid'], line['url']
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['data_result']['ctripGT_detail']
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

def get_ctripPoi_tasks():
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['data_result']['ctripPoi_image_url']
    se = defaultdict(set)
    for co in collections.find({}):
        if len(se[co['imgInfo.poiUrl']]) <=50:
            se[co['imgInfo.poiUrl']].add(co['bigImgUrl'])
            yield str(co['id']),co['bigImgUrl']


def insert_ctripPoi_task():
# --- ctrip Poi
    with InsertTask(worker='proj.total_tasks.images_task', queue='file_downloader', routine_key='file_downloader',
                    task_name='images_total_Poi_20180314a', source='ctripPoi', _type='FileDownloader',
                    priority=11) as it:
        for sid, url in get_ctripPoi_tasks():
            args = {
                'source': "ctripPoi",
                'source_id': sid,
                'target_url': url,
                'bucket_name': 'mioji-attr',
                'file_prefix': 'ctripPoi',
                'is_poi_task': True,
                'need_insert_db': True,
            }
            it.insert_task(args)



def insert_ctripGT_task():
#--- ctrip GT
    with InsertTask(worker='proj.total_tasks.images_task', queue='file_downloader', routine_key='file_downloader',
                    task_name='image_GT_ctrip', source='ctripGT', _type='FileDownloader',
                    priority=11) as it:
        for sid, url in get_GT_tasks():
            args = {
                'source': "ctripGT",
                'source_id': sid,
                'target_url': url,
                'bucket_name': 'mioji-grouptravel',
                'file_prefix': 'ctripGT',
                'is_poi_task': True,
                'need_insert_db': True,
            }
            it.insert_task(args)


def get_data_from_db(sql):
    base_ip = '10.10.230.206'
    base_user = 'mioji_admin'
    base_pwd = 'mioji1109'
    base_db = 'tmp'
    mysql_db_admin = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip, port=3306,
                              user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False,
                              blocking=True)
    db=mysql_db_admin.connection()
    cursor = db.cursor()
    cursor.execute(sql)
    db.close()
    return cursor.fetchall()

def ctripPoiImage_task():
    attr = {1: '2', 2: '5', 3: '3'}
    URL = 'http://you.ctrip.com/Destinationsite/TTDSecond/Photo/AjaxPhotoDetailList?districtId={}&type={}&pindex={}&resource={}'
    datas = get_data_from_db('select poi_id,poi_type, image_num,image_url from ctrip_poi_detail WHERE image_num>=1')
    all_task = []
    with InsertTask(worker='proj.total_tasks.ctripPoi_image_task', queue='supplement_field',
                    routine_key='supplement_field',
                    task_name='ctripPoi_img_20180314a', source='ctripPoi', _type='CityInfo',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for data in datas:
            poi_id = data[0].decode()
            poi_type = attr[data[1]]
            image_num = int(data[2])
            try:
                districtId = re.findall('([0-9]+)', data[-1].decode().split('/')[-2])[0]
            except Exception as e:
                print(e)
                continue
            length = image_num / 40
            if image_num % 40 > 0:
                length += 1
            for l in range(1, int(length + 1)):
                url = URL.format(districtId, poi_type, str(l), poi_id)
                args = {
                    'url': url
                }
                it.insert_task(args)


if __name__ == '__main__':
    #insert_task()
    ctripPoiImage_task()
