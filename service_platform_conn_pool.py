#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/14 上午11:06
# @Author  : Hou Rong
# @Site    : 
# @File    : service_platform_conn_pool.py
# @Software: PyCharm
import pymysql
from DBUtils.PooledDB import PooledDB


def init_pool(host, user, password, database, max_connections=20):
    mysql_db_pool = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=max_connections,
                             host=host, port=3306, user=user, passwd=password,
                             db=database, charset='utf8mb4', blocking=True)
    return mysql_db_pool


db_config = dict(
    user='mioji_admin',
    password='mioji1109',
    host='10.10.228.253',
    database='ServicePlatform'
)

service_platform_pool = init_pool(**db_config)

db_config = dict(
    user='mioji_admin',
    password='mioji1109',
    host='10.10.228.253',
    database='BaseDataFinal',
)

base_data_final_pool = init_pool(**db_config)

db_config = dict(
    user='mioji_admin',
    password='mioji1109',
    host='10.10.228.253',
    database='poi_merge',
)

poi_ori_pool = init_pool(**db_config)

db_config = dict(
    user='reader',
    password='miaoji1109',
    host='10.10.69.170',
    database='base_data',
)

base_data_pool = init_pool(**db_config, max_connections=30)

db_config = dict(
    user='root',
    password='shizuo0907',
    host='10.10.242.173',
    database='data_process',
)

data_process_pool = init_pool(**db_config, max_connections=30)