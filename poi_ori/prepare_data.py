#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/26 下午5:58
# @Author  : Hou Rong
# @Site    : 
# @File    : prepare_data.py
# @Software: PyCharm
import os
import datetime
from poi_ori.update_tag_id import update_tag_id
from service_platform_conn_pool import poi_ori_pool
from logger import get_logger

logger = get_logger("prepare_data")


def prepare_data():
    target_dir = os.path.join('/search/hourong/mv_sql/prepare_data', datetime.datetime.now().strftime("%Y_%m_%d"))

    # 建立 sql 存储文件夹
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # dump data process for bak
    table_time = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    table_names = ['chat_attraction', 'chat_shopping']
    for each_table_name in table_names:
        logger.debug("[start dump sql][host: 10.10.242.173][table name: {}]".format(each_table_name))
        command = "mysqldump -h10.10.242.173 -u root -pshizuo0907 data_process {0} > {1}". \
            format(each_table_name, os.path.join(target_dir, each_table_name + '_' + table_time + '.sql'))
        logger.debug("[load sql][command: {}]".format(command))
        os.system(command)
        logger.debug("[dump sql finished][host: 10.10.242.173][table name: {}]".format(each_table_name))

    # prepare process data
    for each_table_name in table_names:
        logger.debug("[start load sql][host: 10.10.228.253][table name: {}]".format(each_table_name))
        command = "mysql -h10.10.228.253 -umioji_admin -pmioji1109 poi_merge < {1}". \
            format(each_table_name, os.path.join(target_dir, each_table_name + '_' + table_time + '.sql'))
        logger.debug("[load sql][command: {}]".format(command))
        os.system(command)
        logger.debug("[load sql finished][host: 10.10.228.253][table name: {}]".format(each_table_name))


if __name__ == '__main__':
    prepare_data()
